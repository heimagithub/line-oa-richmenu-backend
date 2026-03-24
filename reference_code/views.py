from django.shortcuts import render

# Create your views here.
from django.http.response import JsonResponse
from backend.models import RichmenuList
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated
from io import BytesIO
from django.core.cache import cache
from backend.models import LineProfile, TrackingEvent, UserTag, BackendSetting, LuckyDrawEvent, ClassificationEvent, TrackingEvent, RichmenuItems, Ticket, Source, LuckyDrawItem, RichmenuSchedule, RichmenuAlias
from accounts.models import CompanyBotInfo
from django.http import HttpResponseRedirect, HttpResponse, HttpResponseBadRequest, HttpResponseForbidden
from upload import views as upload_views
from PIL import Image
from datetime import datetime
from django.db import connections
from backend.crypto import AESCipher
from django.db import transaction
import boto3
import base64
import json
import logging
import os
import requests
import random
import string
import uuid
import urllib.parse

logger = logging.getLogger('django')
ciphe = AESCipher(os.environ.get('TOKEN_KEY')[:32])
ciphe_url_par = AESCipher(os.environ.get('URL_PAR_KEY')[:32])
BUCKET_URL = os.environ.get('BUCKET_URL')


def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]


def get_action_data(data):
    info = data.split('&')
    return {"action": info[0].split('=')[1], "data": info[1].split('=')[1]}

# def update_event_action(code, event_code):
#     liff_url = redirect_views.get_redirect_liff_new(code, 'full')
#     liff_url += "?company=" + code + \
#                 "&event=" + event_code + \
#                 "&url=" + event.data


def check_source(source, event_id):
    Source.objects.get_or_create(name=source, tracking_event_id=event_id)
    return source


def use_tracking_url(url, company_code, event_name):
    try:
        event, created = TrackingEvent.objects.get_or_create(data=url, created_by=0)
        if created:
            event.type = 'uri'
            event.code = "".join(random.choice(string.ascii_uppercase + string.digits + string.ascii_lowercase) for _ in range(8))
            event.name = event_name
            event.save()

            res = upload_views.update_short_url(url, "lychee_bot_{}".format(company_code), event.code)
            if res != 200:
                logger.error("更新追蹤網址失敗")
                logger.error(res)
                raise EOFError("更新追蹤網址失敗")
        return event
    except Exception as e:
        logger.error(e)
        return 400

def lucky_draw_check_source(event_id, source):
    items = LuckyDrawItem.objects.filter(wel_msg_lucky_draw_event_id=event_id)
    for item in items:
        if item.tracking_event_id is not None:
            tracking_event_id = item.tracking_event_id
            check_source('{}u{}'.format(source, tracking_event_id), tracking_event_id)


def get_rm_publish(company):
    logger.debug('In get_rm_publish....')
    print('In get_rm_publish....')
    now = datetime.utcnow()
    schedule_dict = {}
    # 以未發佈最近的優先  如果沒有就以最後發佈的顯示
    with connections['default'].cursor() as cur:
        sql = '''SELECT * FROM {}.richmenu_schedule where deleted=0 and link_at > %s order by real_link_at;'''.format(company)
        cur.execute(sql, [now])
        rows = dictfetchall(cur)
        print('rows1')
        print(rows)
        for row in rows:
            id = row.get('richmenu_list_id')
            if id in schedule_dict.keys():
                continue
            link_at = row['link_at']
            if link_at is not None:
                link_at = "{}Z".format(link_at)
            schedule_dict[id] = link_at

        sql = '''SELECT * FROM {}.richmenu_schedule where deleted=0 and real_link_at < %s order by real_link_at desc;'''.format(company)
        cur.execute(sql, [now])
        rows = dictfetchall(cur)
        print('rows2')
        print(rows)
        for row in rows:
            id = row.get('richmenu_list_id')
            if id in schedule_dict.keys():
                continue
            link_at = row['real_link_at']
            if link_at is not None:
                link_at = "{}Z".format(link_at)
            schedule_dict[id] = link_at
    return schedule_dict


def get_rm_status(company):
    now = datetime.utcnow()
    status_dict = {}
    # 以未發佈最近的優先  如果沒有就以最後發佈的顯示
    with connections['default'].cursor() as cur:
        sql = '''SELECT * FROM {}.richmenu_schedule where deleted=0 and link_at > %s order by link_at;'''.format(company)
        cur.execute(sql, [now])
        rows = dictfetchall(cur)
        for row in rows:
            id = row.get('richmenu_list_id')
            status_dict[id] = '待發佈'

        sql = '''SELECT * FROM {}.richmenu_schedule where deleted=0 and link_at<%s and status_code=200 order by link_at desc limit 1;'''.format(company)
        cur.execute(sql, [now])
        rows = dictfetchall(cur)
        if len(rows) > 0:
            status_dict[rows[0]['richmenu_list_id']] = '運行中'

    return status_dict


## 找往上關聯的圖文選單
def get_up_link_rm_dict(company):
    with connections['default'].cursor() as cur:
        sql = f'''SELECT * FROM {company}.richmenu_items where deleted=0 and `type`=11;'''
        cur.execute(sql)
        rows = dictfetchall(cur)

    up_rm_dict = {}
    for row in rows:
        rm_id = row['richmenu_list_id']
        link_rm_id = int(row['content'])
        if link_rm_id not in up_rm_dict.keys():
            up_rm_dict[link_rm_id] = []
        up_rm_dict[link_rm_id].append(rm_id)
    return up_rm_dict


# 把每個圖文選單 第一層連結寫成dict
def get_link_rm_dict(company):
    with connections['default'].cursor() as cur:
        sql = f'''SELECT * FROM {company}.richmenu_items where deleted=0 and `type`=11;'''
        cur.execute(sql)
        rows = dictfetchall(cur)

    rm_dict = {}
    for row in rows:
        rm_id = row['richmenu_list_id']
        if rm_id not in rm_dict.keys():
            rm_dict[rm_id] = []
        link_rm_id = int(row['content'])
        if link_rm_id not in rm_dict[rm_id]:
            rm_dict[rm_id].append(link_rm_id)
    return rm_dict


# 把每個圖文選單第一層連結的dict 遞迴找出某的圖文選單連結的所有圖文選單
def get_linked_rm_list(rm_dict, rm_id, single_layer=False):
    related_list = [rm_id]
    again = True
    while True:
        if not again:
            break
        again = False
        tmp1 = related_list
        logger.debug('related_list:')
        logger.debug(tmp1)
        for i in tmp1:
            if i in rm_dict.keys():
                for j in rm_dict[i]:
                    if j not in related_list:
                        again = True
                        related_list.append(j)
            if single_layer:
                break
    return related_list[1:len(related_list)]


def remove_all_rm_alias(company_code):
    token = get_token(company_code)
    logger.debug('In del_rm_from_line')
    headers = {
        'Authorization': 'Bearer {}'.format(token),
        'Content-Type': "application/json",
    }

    res = requests.get('https://api.line.me/v2/bot/richmenu/alias/list', headers=headers)
    data = json.loads(res.text)
    alias = data.get('aliases')

    for a in alias:
        delete_rm_alias(company_code, a['richMenuAliasId'])
    RichmenuAlias.objects.filter(status=0).update(status=1)


@api_view(['GET'])
@permission_classes((IsAuthenticated,))
def close_all_rm(request):
    company_code = request.COOKIES['lychee_code']
    remove_all_rm(company_code)
    remove_all_rm_alias(company_code)

    return JsonResponse({"status": 200, 'msg': '已成功關閉'})

@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def richmenu_unlink(request):
    data = request.data
    logger.debug(data)
    # schedule_id = data.get('schedule_id')
    company_code = request.COOKIES['lychee_code']
    # schedule = RichmenuSchedule.objects.filter(id=schedule_id).values().last()
    rm_id = data.get('rm_id')
    push_sqs(action="unlink_default_rm",
             pars=json.dumps({"type": "unlink", "rm_id": rm_id}),
             company_code=company_code)

    # if schedule['publish_target'] == '預設圖文選單':
    #     push_sqs(action="unlink_default_rm", pars=json.dumps({"schedule_id": schedule_id, "type": "unlink", "rm_id": schedule['richmenu_list_id']}), company_code=company_code)
    # elif schedule['publish_target'] == '標籤':
    #     push_sqs(action="pre_set_rm_by_tag", pars=json.dumps({"schedule_id": schedule_id, "type": "unlink"}), company_code=company_code)
    return JsonResponse({"status": 200, 'msg': '圖文選單已關閉，作業時間約須1-3分鐘，請稍候關閉作業完成'})

@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def get_rm_status(request):
    data = request.data
    logger.debug(data)
    rm_id = data.get('rm_id')
    schedule = RichmenuSchedule.objects.filter(richmenu_list_id=rm_id).values().last()
    return JsonResponse({"status": 200, 'schedule': schedule})

@api_view(['GET'])
@permission_classes((IsAuthenticated,))
def get_rich_menu_list(request):
    try:
        company = 'lychee_bot_{}'.format(request.COOKIES['lychee_code'])
        logger.debug('In get_rich_menu_list')
        rm_dict = get_link_rm_dict(company)
        logger.debug(rm_dict)
        schedule_dict = get_rm_publish(company)
        with connections['default'].cursor() as cur:
            sql = '''SELECT * FROM {}.richmenu_list where deleted=0 order by id desc;'''.format(company)
            cur.execute(sql)
            rows_menu = dictfetchall(cur)

            ## 找link dict 先做 name dict
            title_dict = {}
            for row in rows_menu:
                title_dict[row.get('id')] = row.get('name')

            rm_list = []
            for rm in rows_menu:
                id = rm.get('id')
                logger.debug(id)
                linked_rm_list = get_linked_rm_list(rm_dict, id, single_layer=True)
                logger.debug(linked_rm_list)
                linked_rm_list_by_title = []
                for linked_rm in linked_rm_list:
                    if linked_rm in title_dict.keys():
                        linked_rm_list_by_title.append(title_dict[linked_rm])
                logger.debug(linked_rm_list_by_title)

                linik_at = ''
                if id in schedule_dict.keys():
                    linik_at = schedule_dict[id]

                image = rm.get('preview_image')
                if image is None:
                    image = rm.get('image_name')

                rm_list.append({"id": id,
                                "title": rm.get('name'),
                                "preview": os.environ.get('RM_IMAGE_URL').format(CODE=request.COOKIES['lychee_code'], NAME=image, BUCKET_URL=BUCKET_URL),
                                "updated_at": "{}Z".format(rm.get('updated_at')),
                                "status": rm.get('status'),
                                "link_list": linked_rm_list_by_title,
                                "richmenu_id": rm.get('richmenu_id'),
                                "link_at": linik_at})

            return JsonResponse({"status": 200,
                                 "rm_list": rm_list})

    except Exception as e:
        logger.debug('Error in get_rich_menu_list')
        logger.error(e)
        return JsonResponse({"status": 400, "msg": "發生問題！請洽荔枝智慧"})


@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def save_rm(request):
    try:
        with transaction.atomic():
            ############################################################
            # 　新增圖文　rollback
            rm_id = request.POST['id']
            name = request.POST['name']
            template_id = request.POST['template_id']
            print('-----------------------')
            print(type(rm_id))
            print(rm_id)
            print('-----------------------')

            if int(rm_id) < 0:
                richmenu = RichmenuList.objects.filter(name=name, deleted=0)
                logger.debug(len(richmenu))
                if len(richmenu) > 0:
                    return JsonResponse({'status': False, "msg": "圖文選單名稱重複!"})
                richmenu = RichmenuList.objects.create(name=name, template=template_id)
            else:
                richmenu = RichmenuList.objects.get(id=rm_id)
                richmenu.name = name

            rm_id = richmenu.id
            logger.debug('rm_id:')
            logger.debug(rm_id)

            background = request.data.get('background')
            if request.data.get('use_ori_img') == '1':
                image = request.data.get('ori_src')
                background = image
            else:
                image = request.data.get('image')

            ## 自訂圖文選單要截圖

            preview_image_name = None
            if template_id != 0:
                image = request.data.get('image')
                image_ = str(image).replace('data:image/jpeg;base64,', '')
                image_ = image_.replace('data:image/png;base64,', '')
                img = Image.open(BytesIO(base64.b64decode(image_)))
                print("img.size:")
                print(img.size)
                if img.size != (2500, 1686):
                    img = img.resize((2500, 1686), Image.ANTIALIAS)
                img = img.convert('RGB')
                buffer = BytesIO()
                img.save(buffer, 'JPEG', optimize=True, quality=60)
                preview_image = base64.b64encode(buffer.getvalue()).decode('UTF-8')
                preview_image_name = str(uuid.uuid4().hex) + '.png'
                preview_image_name1 = 'richmenu/' + request.COOKIES['lychee_code'] + '/' + preview_image_name
                upload_views.image(image_base64=preview_image,
                                   image_name=preview_image_name1)

            ###########################################################################################

            btn_list = json.loads(request.POST['btn_list'])
            RichmenuItems.objects.filter(richmenu_list_id=rm_id).update(deleted=True)

            for idx, btn in enumerate(btn_list):

                logger.debug('here~~~~~~~~~~~~~~`')
                logger.debug(btn)

                ## 負座標偵測
                if btn['config']['width'] < 0:
                    btn['config']['x'] += btn['config']['width']
                    btn['config']['width'] *= -1

                if btn['config']['height'] < 0:
                    btn['config']['y'] += btn['config']['height']
                    btn['config']['height'] *= -1

                ############################################################################################################
                if btn.get('id') < 0:
                    area = idx
                    if btn.get('area') not in ['', None]:
                        area = btn.get('area')

                    content = str(btn.get('content'))
                    if btn['type'] == '6':
                        content = json.dumps({"msg": btn['msg'], "content": btn['content']})

                    item = RichmenuItems.objects.create(richmenu_list_id=rm_id,
                                                        config=json.dumps(btn.get('config')),
                                                        label=btn.get('label'),
                                                        type=btn.get("type"),
                                                        area=area,
                                                        content=content)
                    btn['id'] = item.id
                else:
                    item = RichmenuItems.objects.get(id=btn.get('id'))
                    item.config = json.dumps(btn.get('config'))
                    item.label = btn.get('label')
                    item.type = btn['type']
                    item.deleted = False
                    if btn['type'] == '6':
                        item.content = json.dumps({"msg": btn['msg'], "content": btn['content']})
                    else:
                        item.content = str(btn.get('content'))
                    item.save()

            ## Richmenu
            uid4_image_name = ""
            if background not in [None, '']:
                uid4_image_name = str(uuid.uuid4().hex) + '.png'
                image_name = 'richmenu/' + request.COOKIES['lychee_code'] + '/' + uid4_image_name
                upload_views.image(image_base64=background,
                                   image_name=image_name)

            richmenu.title = request.POST['title']
            if uid4_image_name not in [""]:
                richmenu.image_name = uid4_image_name
            richmenu.show_default = request.POST['show_default'] == 'true'
            richmenu.texts = request.POST['texts']
            richmenu.updated_at = datetime.utcnow()
            richmenu.detail = json.dumps({"font_size": request.POST['font_size'], "color": request.POST['color'],
                                          "show_border": request.POST['show_border']})
            richmenu.preview_image = preview_image_name
            richmenu.save()

            return JsonResponse({"status": True,
                                 "btn_list": btn_list,
                                 "rm_id": rm_id,
                                 "msg": "儲存完成",
                                 })

    except Exception as e:
        if '40000' in str(e):
            logger.debug(e)
            return JsonResponse({"status": False, "msg": "請先到票券模組設定票券！"})
        elif '40001' in str(e):
            logger.debug(e)
            return JsonResponse({"status": False, "msg": "請先到會員系統啟動會員制度！"})
        else:
            logger.error(e)
            return JsonResponse({"status": False, "msg": "儲存失敗！"})


def del_rm_from_line(token, richmenu_id):
    logger.debug('In del_rm_from_line')
    headers = {
        'Authorization': 'Bearer {}'.format(token),
        'Content-Type': "application/json",
    }
    url = '''https://api.line.me/v2/bot/richmenu/{}'''.format(richmenu_id)
    res = requests.delete(url, headers=headers)
    pass


def new_rm_to_line(token, payload):
    logger.debug('In new_rm_to_line')
    headers = {
        'Authorization': 'Bearer {}'.format(token),
        'Content-Type': "application/json",
    }
    print("payload:")
    print(payload)
    response = requests.post("https://api.line.me/v2/bot/richmenu",
                             data=json.dumps(payload),
                             headers=headers)
    logger.debug(response.text)
    res = json.loads(response.text)
    richmenu_id = res.get('richMenuId')
    return richmenu_id


def upload_rm_image_to_line(token, richmenu_id, image_read):
    logger.debug('In upload_rm_image_to_line')
    url = "https://api-data.line.me/v2/bot/richmenu/{}/content".format(richmenu_id)
    headers = {
        'Authorization': 'Bearer {}'.format(token),
        'Content-Type': 'image/png',
    }
    response = requests.post(url,
                             data=image_read,
                             headers=headers)
    logger.debug(response.text)


@api_view(['GET'])
@permission_classes((IsAuthenticated,))
def remove_all_rm_api(request):
    company_code = request.COOKIES['lychee_code']
    remove_all_rm(company_code)
    return JsonResponse({"status": 200})


def remove_all_rm(company_code):
    token = get_token(company_code)
    headers = {
        'Authorization': 'Bearer {}'.format(token),
    }
    res = requests.get('https://api.line.me/v2/bot/richmenu/list', headers=headers)
    tmp = json.loads(res.text)
    logger.debug(tmp)

    # richmenus
    rm_list = tmp['richmenus']
    for rm in rm_list:
        richmenu_id = rm['richMenuId']
        logger.debug(richmenu_id)
        del_rm_from_line(token, richmenu_id)

    RichmenuList.objects.all().update(richmenu_id=None, status=0)

    return JsonResponse({'status': 200})


def get_action(company_code, btn, rm_id):
    logger.debug('In get_action: ')
    logger.debug("btn.type:")
    logger.debug(btn.type)
    logger.debug("btn.content")
    logger.debug(btn.content)

    logger.debug(rm_id)
    item_id = btn.id

    if btn.type == 0:
        logger.debug('btn.type == 0')
        event_tracking = TrackingEvent.objects.get(id=btn.content)
        liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
        source = check_source("r{}u".format(item_id), event_tracking.id)
        liff_url += "?s={}".format(source)
        return {"type": "uri", "uri": liff_url}

    elif btn.type == 1:
        event_lucky = LuckyDrawEvent.objects.get(id=btn.content)
        logger.debug(event_lucky.liff_url)
        liff_url = event_lucky.liff_url

        if "line.me" not in liff_url:
            liff_url_tmp = liff_url.split('?')
            liff_url_tmp1 = liff_url_tmp[1].replace('company', 'c').replace('code', 'co')
            pars = ciphe_url_par.encrypt(liff_url_tmp1).decode('utf-8')
            liff_url = liff_url_tmp[0] + "?s=r{}&pars=".format(item_id) + urllib.parse.quote(pars)
        else:
            event_tracking = use_tracking_url(liff_url, company_code, event_lucky.name)
            liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
            source = check_source("r{}l{}".format(item_id, event_lucky.id), event_tracking.id)
            liff_url += "?s={}".format(source)
            # TODO: source to source table
            lucky_draw_check_source(btn.content, source)
        return {"type": "uri", "uri": liff_url}

    elif btn.type == 2:
        event = ClassificationEvent.objects.get(id=btn.content)
        logger.debug(event.liff_url)
        liff_url = event.liff_url

        if 'line.me' not in liff_url:
            liff_url_tmp = liff_url.split('?')
            liff_url_tmp1 = liff_url_tmp[1].replace('company', 'c').replace('event', 'e')
            pars = ciphe_url_par.encrypt(liff_url_tmp1).decode('utf-8')
            liff_url = liff_url_tmp[0] + "?s=r{}&pars=".format(item_id) + urllib.parse.quote(pars)
        else:
            event_tracking = use_tracking_url(liff_url, company_code, event.name)
            liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
            source = check_source("r{}c{}".format(item_id, event.id), event_tracking.id)
            liff_url += "?s={}".format(source)
        return {"type": "uri", "uri": liff_url}

    elif btn.type == 3:
        return {"type": "message", "text": btn.content}

    elif btn.type == 4:
        if btn.content in [0, 'share', '0']:
            share = BackendSetting.objects.get(name='invitation_share_liff')
            share_info = json.loads(share.data)
            liff_url = share_info.get('liff_url')

            event_tracking = use_tracking_url(liff_url, company_code, '分享活動')
            liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
            source = check_source("r{}i0".format(item_id), event_tracking.id)
            liff_url += "?s={}".format(source)

        elif btn.content in [1, 'show', '1']:
            show = BackendSetting.objects.get(name='invitation_show_liff')
            show_info = json.loads(show.data)
            liff_url = show_info.get('liff_url')

            # 還在v1 則不加工
            if 'line://app/' not in liff_url:
                liff_url = liff_url.split('?')
                liff_url = liff_url[0] + '?s=r{}&'.format(rm_id) + liff_url[1]
            event_tracking = use_tracking_url(liff_url, company_code, '成就看板')
            liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
            source = check_source("r{}s0".format(item_id), event_tracking.id)
            liff_url += "?s={}".format(source)
        return {"type": "uri", "uri": liff_url}

    elif btn.type == 5:
        try:
            item = BackendSetting.objects.get(name='member_liff_url')
        except BackendSetting.DoesNotExist:
            return JsonResponse({"status": False, "msg": "尚未設定會員資料填寫模組！"})
        except Exception as e:
            logger.error(e)
            return JsonResponse({"status": False, "msg": "設定會員資料填寫模組未知錯誤！"})

        if item.data is None:
            return JsonResponse({"status": False, "msg": "尚未設定會員資料填寫模組！"})
        else:
            liff_url = item.data
            event_tracking = use_tracking_url(liff_url, company_code, '會員資料填寫模組')
            liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
            source = check_source("r{}d0".format(item_id), event_tracking.id)
            liff_url += "?s={}".format(source)
            return {"type": "uri", "uri": liff_url}

    elif btn.type == 6:
        tmp = json.loads(btn.content)
        return {"type": 'postback', "text": tmp['msg'], "data": tmp['content']}

    elif btn.type == 7:
        return {"type": "uri", "uri": btn.content}

    elif btn.type == 8:
        setting_url = BackendSetting.objects.filter(name='notify_訂閱追蹤')
        url_data = json.loads(setting_url[0].data)
        liff_url = url_data.get('liff_url')

        event_tracking = use_tracking_url(liff_url, company_code, 'notify_訂閱追蹤')
        liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
        source = check_source("r{}n0".format(item_id), event_tracking.id)
        liff_url += "?s={}".format(source)
        return {"type": "uri", "uri": liff_url}

    elif btn.type == 9:
        setting = BackendSetting.objects.filter(name='ticket_liff_url')
        if len(setting) != 1 or setting[0].data is None:
            raise EOFError('40000')

        setting_data = json.loads(setting[0].data)
        liff_url = setting_data.get('liff_url')

        event_tracking = use_tracking_url(liff_url, company_code, '票券匣')
        liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
        source = check_source("r{}t0".format(item_id), event_tracking.id)
        liff_url += "?s={}".format(source)
        return {"type": "uri", "uri": liff_url}

    ## 會員系統
    elif btn.type == 10:
        setting = BackendSetting.objects.filter(name='loyalty_info')
        if len(setting) != 1 or setting[0].data is None:
            raise EOFError('40001')

        setting_data = json.loads(setting[0].data)
        liff_url = setting_data.get('liff_url')

        event_tracking = use_tracking_url(liff_url, company_code, '會員系統')
        liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
        source = check_source("r{}z0".format(item_id), event_tracking.id)
        liff_url += "?s={}".format(source)
        return {"type": "uri", "uri": liff_url}

    elif btn.type == 11:
        code = "".join(random.choice(string.digits + string.ascii_lowercase) for _ in range(8))
        RichmenuAlias.objects.create(richmenu_items_id=btn.id, code=code)
        rm_id = btn.content
        logger.debug("rm_id:")
        logger.debug(rm_id)

        rm = RichmenuList.objects.get(id=rm_id)
        richmenu_id = rm.richmenu_id
        if richmenu_id is None:
            richmenu_id = set_rm_to_line(company_code, rm_id)
            logger.debug("richmenu_id")
            logger.debug(richmenu_id)
            if richmenu_id == 400:
                return 400

        res = set_rm_alias(company_code, code, richmenu_id)
        logger.debug("res:")
        logger.debug(res)

        return {
            "type": "richmenuswitch",
            "richMenuAliasId": code,
            "data": code
        }


def get_token(company_code):
    logger.debug("In get_token")
    logger.debug(company_code)
    token_dict = cache.get("token_dict")
    decode_token = ciphe.decrypt(token_dict[company_code]).decode('utf-8')
    return decode_token


def set_rm_alias(company_code, code, richmenu_id):
    token = get_token(company_code)
    headers = {
        'Authorization': 'Bearer {}'.format(token),
        'Content-Type': "application/json",
    }
    payload = {
        "richMenuAliasId": code,
        "richMenuId": richmenu_id,
    }
    logger.debug("payload:")
    logger.debug(payload)
    response = requests.post("https://api.line.me/v2/bot/richmenu/alias",
                             data=json.dumps(payload),
                             headers=headers)
    return response.status_code


##########################################################################################
sqs = boto3.client('sqs',
                    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                    aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
                    region_name=os.getenv('AWS_REGION'))


def push_sqs(action, pars, company_code):
    logger.debug(action)
    logger.debug(pars)
    logger.debug(company_code)
    logger.debug(os.getenv('QUEUE_URL_API'))
    resp = sqs.send_message(
        QueueUrl=os.getenv('QUEUE_URL_API'),
        MessageBody=json.dumps({
            "a": action,
            "p": pars,
            "c": company_code,
        })
    )
    return True
##########################################################################################
def check_before_link(data):
    publish_target = data.get('publish_target')
    target_list = data.get('target_list')
    msg = ''
    if publish_target in ['標籤']:
        rows = UserTag.objects.filter(tag_id__in=target_list).count()
        if rows == 0:
            msg = '發佈對象人數為0，請另行選擇發佈對象，重新發佈'
    return msg



@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def set_rm_to_line_api(request):
    data = request.data
    company_code = request.COOKIES['lychee_code']
    logger.debug("############################################################")
    logger.debug("data:")
    logger.debug(data)
    logger.debug("############################################################")
    rm_id = data.get('rm_id')

    ## 檢查如果是tag 且tag沒有人就回傳
    msg = check_before_link(data)
    if msg != '':
        return JsonResponse({'status': 403, 'msg': msg})

    publish_target = data.get('publish_target')
    target_list = data.get('target_list')
    link_radio = data.get('link_radio')
    date = data.get('date')
    if date == '':
        date = None

    schedule = RichmenuSchedule.objects.create(richmenu_list_id=rm_id,
                                               link_radio=link_radio,
                                               link_at=date,
                                               publish_target=publish_target,
                                               target_list=json.dumps(target_list))
    schedule_id = schedule.id
    logger.debug(schedule_id)

    rm = RichmenuList.objects.get(id=rm_id)
    rm.status = 1
    rm.save()

    if os.getenv('LOCAL_FLAG') == '1' and os.getenv('DEV_FLAG') == '0':
        return JsonResponse({'status': 400, 'msg': 'Local 無法測試'})

    payload = {
        'company_code': company_code,
        'schedule_id': schedule_id
    }
    if link_radio == 0:
        logger.debug(os.getenv('LYCHEEAPI_URL') + '/richmenu_link_set_to_queue')
        res = requests.post(os.getenv('LYCHEEAPI_URL') + '/richmenu_link_set_to_queue', json.dumps(payload))
        logger.debug(res.status_code)
        logger.debug(res.text)
        if res.status_code == 200:
            content = json.loads(res.text)
            if content['status'] != 200:
                return JsonResponse({'status': 402, 'msg': '發佈出現問題402'})
        else:
            return JsonResponse({'status': 401, 'msg': '發佈出現問題401'})
    else:
        logger.debug('In assign rm')
        assign_date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
        assign_date = str(assign_date)[0:16]
        assign_date = assign_date.replace('-', '').replace(' ', '').replace(':', '')
        status = schedule_create(assign_date,
                                 os.getenv('LYCHEEAPI_URL') + '/richmenu_link_set_to_queue',
                                 'POST',
                                 payload)
        if status is None:
            return JsonResponse({'status': 403, "msg": "排程發生問題！ 請洽荔枝智慧"})

    return JsonResponse({'status': 200, "msg": "圖文選單已發佈，作業時間約須1-3分鐘，請稍候發佈作業完成"})


def set_rm_to_line(company_code, rm_id):
    try:
        logger.debug(company_code)
        logger.debug(rm_id)

        rm = RichmenuList.objects.get(id=rm_id)
        rm_items = RichmenuItems.objects.filter(richmenu_list_id=rm_id)
        areas = []
        for btn in rm_items:
            print(btn.type)
            config = json.loads(btn.config)
            tmp = {}
            tmp["bounds"] = {"x": config['x'] * 4, "y": config['y'] * 4, "width": config['width'] * 4, "height": config['height'] * 4}
            action = get_action(company_code, btn, rm_id)
            print('action:')
            print(action)
            tmp["action"] = action
            areas.append(tmp)

        payload = {
            "size": {
                "width": 2500,
                "height": 1686
            },
            "selected": False,
            "name": "richmenu-{}".format(rm_id),
            "chatBarText": rm.title,
            "areas": areas
        }

        decode_token = get_token(company_code)

        logger.debug("payload:")
        logger.debug(payload)

        logger.debug("rm.richmenu_id:")
        logger.debug(rm.richmenu_id)

        if rm.richmenu_id is not None:
            del_rm_from_line(decode_token, rm.richmenu_id)

        richmenu_id = new_rm_to_line(decode_token, payload)
        rm.richmenu_id = richmenu_id
        rm.status = 0
        rm.save()

        url = os.environ.get('RM_IMAGE_URL').format(CODE=company_code,
                                                    NAME=rm.image_name,
                                                    BUCKET_URL=BUCKET_URL)
        res = requests.get(url)
        image_base64 = BytesIO(res.content)
        image_read = image_base64.read()
        upload_rm_image_to_line(decode_token, richmenu_id, image_read)
        return richmenu_id
    except Exception as e:
        logger.error('Error in set_rm_to_line')
        logger.error(e)
        return 400


@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def test_richmenu(request):
    try:
        with transaction.atomic():
            ############################################################
            #　新增圖文　rollback
            rm_id = request.POST['id']
            name = request.POST['name']
            template_id = request.POST['template_id']
            print('-----------------------')
            print(type(rm_id))
            print(rm_id)
            print('-----------------------')

            if CompanyBotInfo.objects.get(id=1).token is None:
                return JsonResponse({"status": 401, "msg": '尚未連接TOKEN！ 請先連接TOKEN'})

            if int(rm_id) < 0:
                richmenu = RichmenuList.objects.filter(name=name, deleted=0)
                logger.debug(len(richmenu))
                if len(richmenu) > 0:
                    return JsonResponse({'status': False, "msg": "圖文選單名稱重複!"})

                richmenu = RichmenuList.objects.create(name=name, template=template_id)
            else:
                richmenu = RichmenuList.objects.get(id=rm_id)
                richmenu.name = name

            logger.debug('new is ok')
            ############################################################
            rm_id = richmenu.id
            print('rm_id:')
            print(rm_id)

            background = request.data.get('background')

            if request.data.get('use_ori_img') == '1':
                logger.debug('In use_ori_img~~~')
                image = request.data.get('ori_src')
                background = image
            else:
                logger.debug('In use_ori_img else')
                image = request.data.get('image')

            ## 自訂圖文選單要截圖
            preview_image_name = None
            if template_id != 0:
                image = request.data.get('image')
                image_ = str(image).replace('data:image/jpeg;base64,', '')
                image_ = image_.replace('data:image/png;base64,', '')
                img = Image.open(BytesIO(base64.b64decode(image_)))
                print("img.size:")
                print(img.size)
                if img.size != (2500, 1686):
                    img = img.resize((2500, 1686), Image.ANTIALIAS)
                img = img.convert('RGB')
                buffer = BytesIO()
                img.save(buffer, 'JPEG', optimize=True, quality=60)
                preview_image = base64.b64encode(buffer.getvalue()).decode('UTF-8')

                preview_image_name = str(uuid.uuid4().hex) + '.png'
                preview_image_name1 = 'richmenu/' + request.COOKIES['lychee_code'] + '/' + preview_image_name
                upload_views.image(image_base64=preview_image,
                                   image_name=preview_image_name1)

            image_ = str(image).replace('data:image/jpeg;base64,', '')
            image_ = image_.replace('data:image/png;base64,', '')

            img = Image.open(BytesIO(base64.b64decode(image_)))

            print("img.size:")
            print(img.size)

            if img.size != (2500, 1686):
                # contentLength = None
                img = img.resize((2500, 1686), Image.ANTIALIAS)


            img_file = BytesIO()
            img = img.convert('RGB')
            img.save(img_file, 'JPEG', optimize=True, quality=60)

            image_base64_ = BytesIO(img_file.getvalue())
            image_read = image_base64_.read()
            btn_list = json.loads(request.POST['btn_list'])
            # for pos in request.data.get('pos_dict'):
            #     pass

            code = request.COOKIES['lychee_code']
            decode_token = get_token(code)

            areas = []
            print("btn_list:")
            print(btn_list)

            # raise EOFError('test error')

            for idx, btn in enumerate(btn_list):

                logger.debug('here~~~~~~~~~~~~~~`')
                logger.debug(btn)

                ## 負座標偵測
                if btn['config']['width'] < 0:
                    btn['config']['x'] += btn['config']['width']
                    btn['config']['width'] *= -1

                if btn['config']['height'] < 0:
                    btn['config']['y'] += btn['config']['height']
                    btn['config']['height'] *= -1

                tmp = {
                    "bounds": {"x": btn['config']['x'] * 4,
                               "y": btn['config']['y'] * 4,
                               "width": btn['config']['width'] * 4,
                               "height": btn['config']['height'] * 4}
                }

                ############################################################################################################
                if btn.get('id') < 0:
                    area = idx
                    if btn.get('area') not in ['', None]:
                        area = btn.get('area')

                    content = str(btn.get('content'))
                    if btn['type'] == '6':
                        content = json.dumps({"msg": btn['msg'], "content": btn['content']})

                    item = RichmenuItems.objects.create(richmenu_list_id=rm_id,
                                                        config=json.dumps(btn.get('config')),
                                                        label=btn.get('label'),
                                                        type=btn.get("type"),
                                                        area=area,
                                                        content=content)
                    btn['id'] = item.id
                else:
                    item = RichmenuItems.objects.get(id=btn.get('id'))
                    item.config = json.dumps(btn.get('config'))
                    item.label = btn.get('label')
                    item.type = btn['type']
                    if btn['type'] == '6':
                        item.content = json.dumps({"msg": btn['msg'], "content": btn['content']})
                    else:
                        item.content = str(btn.get('content'))
                    item.save()

                item_id = item.id
                ############################################################################################################

                ## type 0: uri
                ## type 1: 抽獎
                ## type 2: 訂閱
                ## type 3: 關鍵字
                ## type 4: 分享邀請
                ## type 5: 會員資料填寫
                ## type 6: postback
                ## type 7: 不追總網址
                ## type 8: notify 訂閱
                ## type 9: 票券

                if btn['type'] == '0':
                    event_tracking = TrackingEvent.objects.get(id=btn.get('content'))
                    liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
                    source = check_source("r{}u".format(item_id), event_tracking.id)
                    liff_url += "?s={}".format(source)

                    logger.debug('liff_url')
                    logger.debug(liff_url)
                    tmp['action'] = {"type": "uri", "uri": liff_url}

                elif btn['type'] == '1':
                    logger.debug('lottery')
                    logger.debug(btn.get('content'))
                    event_lucky = LuckyDrawEvent.objects.get(id=btn.get('content'))
                    logger.debug(event_lucky.liff_url)
                    liff_url = event_lucky.liff_url

                    if "line.me" not in liff_url:
                        liff_url_tmp = liff_url.split('?')
                        liff_url_tmp1 = liff_url_tmp[1].replace('company', 'c').replace('code', 'co')
                        pars = ciphe_url_par.encrypt(liff_url_tmp1).decode('utf-8')
                        liff_url = liff_url_tmp[0] + "?s=r{}&pars=".format(item_id) + urllib.parse.quote(pars)
                    else:
                        event_tracking = use_tracking_url(liff_url, code, event_lucky.name)
                        liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
                        source = check_source("r{}l{}".format(item_id, event_lucky.id), event_tracking.id)
                        liff_url += "?s={}".format(source)
                        # TODO: source to source table
                        lucky_draw_check_source(btn.get('content'), source)

                    logger.debug('liff_url')
                    logger.debug(liff_url)
                    tmp['action'] = {"type": "uri",
                                     "uri": liff_url}

                elif btn['type'] == '2':
                    logger.debug(btn.get('content'))
                    event = ClassificationEvent.objects.get(id=btn.get('content'))
                    logger.debug(event.liff_url)
                    liff_url = event.liff_url

                    if 'line.me' not in liff_url:
                        liff_url_tmp = liff_url.split('?')
                        liff_url_tmp1 = liff_url_tmp[1].replace('company', 'c').replace('event', 'e')
                        pars = ciphe_url_par.encrypt(liff_url_tmp1).decode('utf-8')
                        liff_url = liff_url_tmp[0] + "?s=r{}&pars=".format(item_id) + urllib.parse.quote(pars)
                    else:
                        event_tracking = use_tracking_url(liff_url, code, event.name)
                        liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
                        source = check_source("r{}c{}".format(item_id, event.id), event_tracking.id)
                        liff_url += "?s={}".format(source)

                    logger.debug('liff_url')
                    logger.debug(liff_url)
                    tmp['action'] = {"type": "uri",
                                     "uri": liff_url}

                elif btn['type'] == '3':
                    tmp['action'] = {"type": "message",
                                     "text": btn['content']}

                elif btn['type'] == '4':
                    if btn.get('content') in [0, 'share']:
                        share = BackendSetting.objects.get(name='invitation_share_liff')
                        share_info = json.loads(share.data)
                        liff_url = share_info.get('liff_url')

                        event_tracking = use_tracking_url(liff_url, code, '分享活動')
                        liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
                        source = check_source("r{}i0".format(item_id), event_tracking.id)
                        liff_url += "?s={}".format(source)

                    elif btn.get('content') in [1, 'show']:
                        show = BackendSetting.objects.get(name='invitation_show_liff')
                        show_info = json.loads(show.data)
                        liff_url = show_info.get('liff_url')

                        # 還在v1 則不加工
                        if 'line://app/' not in liff_url:
                            liff_url = liff_url.split('?')
                            liff_url = liff_url[0] + '?s=r{}&'.format(rm_id) + liff_url[1]
                        event_tracking = use_tracking_url(liff_url, code, '成就看板')
                        liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
                        source = check_source("r{}s0".format(item_id), event_tracking.id)
                        liff_url += "?s={}".format(source)

                    tmp['action'] = {"type": "uri",
                                     "uri": liff_url}

                elif btn['type'] == '5':
                    try:
                        item = BackendSetting.objects.get(name='member_liff_url')
                    except BackendSetting.DoesNotExist:
                        return JsonResponse({"status": False, "msg": "尚未設定會員資料填寫模組！"})
                    except Exception as e:
                        logger.error(e)
                        return JsonResponse({"status": False, "msg": "設定會員資料填寫模組未知錯誤！"})

                    if item.data is None:
                        return JsonResponse({"status": False, "msg": "尚未設定會員資料填寫模組！"})
                    else:
                        liff_url = item.data
                        event_tracking = use_tracking_url(liff_url, code, '會員資料填寫模組')
                        liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
                        source = check_source("r{}d0".format(item_id), event_tracking.id)
                        liff_url += "?s={}".format(source)

                        tmp['action'] = {"type": "uri",
                                         "uri": liff_url}

                elif btn['type'] == '6':
                    tmp['action'] = {
                        "type": 'postback',
                        "text": btn['msg'],
                        "data": btn['content']
                    }

                elif btn['type'] == '7':
                    if 'content' in btn.keys() and btn['content'] != '':
                        tmp['action'] = {"type": "uri",
                                         "uri": btn['content']}
                    else:
                        return JsonResponse({"status": False, "msg": "不追蹤網址不可空白！"})

                elif btn['type'] == '8':
                    setting_url = BackendSetting.objects.filter(name='notify_訂閱追蹤')
                    if len(setting_url) != 1:
                        return JsonResponse({"status": False, "msg": "請先設定 Notify 基本資料！"})

                    url_data = json.loads(setting_url[0].data)
                    liff_url = url_data.get('liff_url')

                    # liff_url = liff_url.split('?')
                    # liff_url = liff_url[0] + '?s=r{}&'.format(rm_id) + liff_url[1]

                    event_tracking = use_tracking_url(liff_url, code, 'notify_訂閱追蹤')
                    liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
                    source = check_source("r{}n0".format(item_id), event_tracking.id)
                    liff_url += "?s={}".format(source)

                    tmp['action'] = {"type": "uri",
                                     "uri": liff_url}

                elif btn['type'] == '9':
                    setting = BackendSetting.objects.filter(name='ticket_liff_url')
                    if len(setting) != 1 or setting[0].data is None:
                        raise EOFError('40000')

                    setting_data = json.loads(setting[0].data)
                    liff_url = setting_data.get('liff_url')

                    # liff_url = liff_url.split('?')
                    # liff_url = liff_url[0] + '?s=r{}&'.format(rm_id) + liff_url[1]

                    event_tracking = use_tracking_url(liff_url, code, '票券匣')
                    liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
                    source = check_source("r{}t0".format(item_id), event_tracking.id)
                    liff_url += "?s={}".format(source)

                    tmp['action'] = {"type": "uri",
                                     "uri": liff_url}

                ## 會員系統
                elif btn['type'] == '10':
                    setting = BackendSetting.objects.filter(name='loyalty_info')
                    if len(setting) != 1 or setting[0].data is None:
                        raise EOFError('40001')

                    setting_data = json.loads(setting[0].data)
                    liff_url = setting_data.get('liff_url')

                    event_tracking = use_tracking_url(liff_url, code, '會員系統')
                    liff_url = os.getenv("TRACK_SHORT_URL") + event_tracking.code
                    source = check_source("r{}z0".format(item_id), event_tracking.id)
                    liff_url += "?s={}".format(source)

                    tmp['action'] = {"type": "uri",
                                     "uri": liff_url}
                ## 更換圖文選單
                elif btn['type'] == '11':
                    alias = "rm_{}_{}_{}".format(rm_id, btn['content'], idx)
                    tmp['action'] = {
                        "type": "richmenuswitch",
                        "richMenuAliasId": alias,
                        "data": alias
                    }

                areas.append(tmp)

            logger.debug('areas')
            logger.debug(areas)

            ##############################################
            url = "https://api.line.me/v2/bot/richmenu"
            token = "Bearer " + decode_token
            show_default = request.POST.get('show_default')
            logger.debug("show_default:")
            logger.debug(show_default)
            if show_default is None:
                show_default = True

            payload = {"size": {
                "width": 2500,
                "height": 1686
            },
                "selected": show_default,
                "name": request.POST['name'],
                "chatBarText": request.POST['title'],
                "areas": areas
            }

            ## Richmenu
            uid4_image_name = ""

            if background not in [None, '']:
                uid4_image_name = str(uuid.uuid4().hex) + '.png'
                image_name = 'richmenu/' + request.COOKIES['lychee_code'] + '/' + uid4_image_name
                upload_views.image(image_base64=background,
                                   image_name=image_name)

            if richmenu.richmenu_id not in [None, '']:
                delete_rmId(token, richmenu.richmenu_id)

            # richmenu = RichmenuList.objects.get(id=request.POST['id'])
            richmenu.title = request.POST['title']
            if uid4_image_name not in [""]:
                richmenu.image_name = uid4_image_name
            richmenu.show_default = request.POST['show_default'] == 'true'
            richmenu.richmenu_id = richmenu_id
            richmenu.texts = request.POST['texts']
            richmenu.updated_at = datetime.utcnow()
            richmenu.detail = json.dumps({"font_size": request.POST['font_size'], "color": request.POST['color'], "show_border": request.POST['show_border']})
            richmenu.preview_image = preview_image_name
            richmenu.save()

            return JsonResponse({"status": True,
                                 "btn_list": btn_list,
                                 # "richmenu_id": richmenu_id,
                                 "rm_id": rm_id,
                                 "msg": "儲存完成",
                                 })

    except Exception as e:
        if '40000' in str(e):
            logger.debug(e)
            return JsonResponse({"status": False, "msg": "請先到票券模組設定票券！"})
        elif '40001' in str(e):
            logger.debug(e)
            return JsonResponse({"status": False, "msg": "請先到會員系統啟動會員制度！"})
        else:
            logger.error(e)
            return JsonResponse({"status": False, "msg": "儲存失敗！"})

def delete_rmId(token, rm_id):
    logger.debug('In delete_rmId:')
    logger.debug(rm_id)
    try:
        headers = {
            'Authorization': token,
        }
        response = requests.delete('https://api.line.me/v2/bot/richmenu/{}'.format(rm_id), headers=headers)
        logger.debug(response.status_code)
        logger.debug(response.text)
        return response.status_code
    except Exception as e:
        logger.error(e)
        return 400


def create_new_version(list_id, btn_list):
    tmp = []
    type_dict = {'uri': 0, 'lottery': 1, 'classification': 2, 'keyword': 3, 'invitation': 4, 'member': 5}
    for key, val in btn_list.items():
        pos = {"x": val['rect']['startX'],
               "y": val['rect']['startY'],
               "width": val['rect']['w'],
               "height": val['rect']['h'],
               "stroke": "blue",
               "fill": "#00000000",
               "strokeWidth": 1,
               "draggable": True,
               "visible": True,
               "click_area": True,
               "name": "".join(random.choice(string.digits) for _ in range(10))}

        content = val.get('content')
        if content == 'share':
            content = '0'
        if content == 'page':
            content = '1'

        tmp.append(RichmenuItems(richmenu_list_id=list_id,
                                 config=json.dumps(pos),
                                 label=key,
                                 type=type_dict[val.get('type')],
                                 area=len(tmp),
                                 content=str(content)))
    RichmenuItems.objects.bulk_create(tmp)


@api_view(['POST'])
@permission_classes((IsAuthenticated, ))
def get_rm(request):

    try:
        print('000000000')

        data = request.data
        rm_id = data.get('id')
        rm = RichmenuList.objects.get(id=rm_id, deleted=False)

        print('1111111111')
        rm_items = RichmenuItems.objects.filter(richmenu_list_id=data.get('id'))
        if len(rm_items) == 0 and rm.detail is not None:
            print('aaaaaa')
            create_new_version(rm.id, json.loads(rm.detail))
            print('bbbbbb')
            rm_items = RichmenuItems.objects.filter(richmenu_list_id=data.get('id'), deleted=False)
        else:
            rm_items = RichmenuItems.objects.filter(richmenu_list_id=data.get('id'), deleted=False)

        ## id
        id = rm.id
        name = rm.name
        image_name = None
        if rm.image_name is not None:
            image_name = rm.image_name

        ## btn
        options_btn_list = []
        for btn in rm_items:
            content = btn.content
            if content.isdigit():
                content = int(content)

            print('id', btn.id)

            tmp = {
                "id": btn.id,
                "label": btn.label,
                "type": str(btn.type),
                "content": content,
                "area": btn.area,
                "config": json.loads(btn.config)
            }

            if tmp['type'] == '6':
                content = json.loads(btn.content)
                tmp.update({"msg": content['msg'],
                            "content": content['content']})

            options_btn_list.append(tmp)

        texts = []
        if rm.texts not in [None, '']:
            texts = json.loads(rm.texts)

        font_size = 18
        color = "black"
        show_border = False
        if rm.detail is not None:
            detail = json.loads(rm.detail)
            font_size = detail.get('font_size')
            color = detail.get('color')
            if detail.get('show_border') in [True, "True", 'true']:
                show_border = True

        return JsonResponse({"status": True,
                             "id": id,
                             "name": name,
                             "rm_title": rm.title,
                             "richmenu_id": rm.richmenu_id,
                             "template": rm.template,
                             "image_name": image_name,
                             "options_btn_list": options_btn_list,
                             "texts": texts,
                             "show_default": rm.show_default,
                             "font_size": font_size,
                             "color": color,
                             "show_border": show_border})

    except Exception as e:
        logger.error(e)
        return JsonResponse({"status": False, "msg": "發生問題！"})

@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def get_booking_list(request):
    data = request.data
    rm_id = data.get('rm_id')
    try:
        ## get booing
        now = datetime.utcnow()
        tmp = RichmenuSchedule.objects.filter(richmenu_list_id=rm_id, deleted=0, status_code=None, link_at__gt=now)
        booking_list = []
        for t in tmp:
            booking_list.append({'booking_time': t.link_at})
        return JsonResponse({"status": 200, 'booking_list': booking_list})

    except Exception as e:
        logger.error(e)
        return JsonResponse({"status": False, "msg": "發生問題！"})


@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def cancel_booking(request):
    data = request.data
    rm_id = data.get('rm_id')
    link_at = data.get('booking_time')

    try:
        RichmenuList.objects.filter(id=rm_id).update(status=0)
        schedule = RichmenuSchedule.objects.filter(richmenu_list_id=rm_id, deleted=0).last()
        schedule.deleted = True
        schedule.status = 3
        schedule.save()
        return JsonResponse({"status": 200})

    except Exception as e:
        logger.error(e)
        return JsonResponse({"status": False, "msg": "發生問題！"})

@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def test_richmenu_link(request):
    try:
        data = request.data
        id_ = data.get('id')
        date = data.get('date')
        company_code = request.COOKIES['lychee_code']
        logger.debug('==========================')
        logger.debug(id_)
        logger.debug(date)
        logger.debug('==========================')

        # remove_all_rm(company_code)
        # remove_all_rm_alias(company_code)

        if date is None:
            push_sqs(action="richmenu_link", pars=json.dumps({"rm_id": id_}), company_code=company_code)
            return JsonResponse({'status': 200, "msg": "圖文選單已發佈，作業時間約須1-3分鐘，請稍候發佈作業完成"})

        ## 排程綁定
        else:
            rm = RichmenuList.objects.get(id=id_)
            schedule_list = RichmenuSchedule.objects.filter(richmenu_list_id=id_, link_at__gt=datetime.utcnow(), deleted=False)
            if len(schedule_list) > 0:
                return JsonResponse({'status': 401, "msg": "已存在排程推播了！"})

            assign_date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ')
            assign_date = str(assign_date)[0:16]
            schedule = RichmenuSchedule.objects.create(richmenu_list_id=id_, link_at=assign_date)
            assign_date = assign_date.replace('-', '').replace(' ', '').replace(':', '')
            status = schedule_create(assign_date, os.getenv('LYCHEEAPI_URL')+'/richmenu_link', 'POST', {'company_code': request.COOKIES['lychee_code'], 'id': schedule.id})
            if status is None:
                return JsonResponse({'status': 403, "msg": "排程發生問題！ 請洽荔枝智慧"})

            rm.status = 1
            rm.save()
            logger.debug("status:")
            logger.debug(status)
            return JsonResponse({'status': status, "msg": "預約成功！"})

    except Exception as e:
        logger.error(e)
        return JsonResponse({'status': False, "msg": "發生問題！請洽荔枝智慧!"})

@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def new_richmenu(request):
    try:
        data = request.data
        logger.debug(data)

        logger.debug('重複偵測')
        richmenu = RichmenuList.objects.filter(name=data.get('label'), deleted=0)
        logger.debug(len(richmenu))
        if len(richmenu) > 0:
            return JsonResponse({'status': False, "msg": "圖文選單名稱重複!"})

        richmenu = RichmenuList.objects.create(name=data.get('label'),
                                               template=data.get('template_id'))
        return JsonResponse({'status': True,
                             "id": richmenu.id,
                             "label": richmenu.name})

    except Exception as e:
        logger.error(e)
        return JsonResponse({'status': False,  "msg": "發生異常! 請洽荔枝智慧"})


@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def del_richmenu(request):
    try:
        data = request.data
        rm = RichmenuList.objects.get(id=data.get('id'))
        if rm.richmenu_id is not None:
            token_dict = cache.get('token_dict')
            code = request.COOKIES['lychee_code']
            decode_token = ciphe.decrypt(token_dict[code]).decode('utf-8')
            status_code = delete_rmId(f'Bearer {decode_token}', rm.richmenu_id)
            logger.debug(status_code)
            if status_code == 200:
                rm.richmenu_id = None
        rm.deleted = True
        rm.save()

        return JsonResponse({'status': True, "msg": "刪除成功!"})

    except Exception as e:
        logger.error(e)
        return JsonResponse({'status': False, "msg": "刪除失敗!"})

@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def del_richmenu_btn(request):
    try:
        data = request.data
        print('=================')
        print(json.dumps(data))
        print('=================')
        item = RichmenuItems.objects.get(id=data.get('btn_id'))
        item.deleted = True
        item.save()

        return JsonResponse({'status': True, "msg": "刪除成功!"})

    except Exception as e:
        logger.error(e)
        return JsonResponse({'status': False, "msg": "刪除失敗!"})

@api_view(['POST'])
@permission_classes((IsAuthenticated,))
def richmenu_link(request):
    try:
        # data = json.loads(request.body.decode('utf-8'))
        data = request.data
        rm = RichmenuList.objects.values('richmenu_id').get(name=data['name'])

        token_dict = cache.get('token_dict')
        code = request.COOKIES['lychee_code']
        decode_token = get_token(code)

        # set_default_richmenu
        try:
            logger.debug(rm['richmenu_id'])
            headers = {
                'Authorization': "Bearer "+decode_token,
                'Content-Type': "application/json",
            }
            url = "https://api.line.me/v2/bot/user/all/richmenu/{richMenuId}".format(richMenuId=rm['richmenu_id'])
            response = requests.post(url, headers=headers)
            logger.debug(response.text)
        except Exception as e:
            logger.error(e)

        ## 個別綁
        # if link_user(richmenu_id=rm['richmenu_id'], token=decode_token):
        #     return JsonResponse({'status': True})
        # else:
        #     return JsonResponse({'status': False})

        return JsonResponse({'status': True})


    except Exception as e:
        return JsonResponse({'status':False})



def link_user(user_id_list=None, richmenu_id=None, token=None):
    try:

        authorization = "Bearer {}".format(token)
        if user_id_list is None:
            profile = LineProfile.objects.select_related('user').values('line_id', 'name').exclude(user__status='2')
        else:
            profile = LineProfile.objects.values('line_id', 'name').filter(user_id__in=user_id_list)

        idx = 0
        users_list = []
        tmp = []
        while True:
            # logger.debug(profile[idx]['line_id'])

            if len(tmp) == 149 or idx+1 == len(profile):
                if profile[idx]['name'] != 'Lychee_Robot':
                    tmp.append(profile[idx]['line_id'])
                users_list.append(tmp)
                if idx+1 == len(profile):
                    break
                tmp = []
            else:
                ###########
                if profile[idx]['name'] == 'Lychee_Robot':
                    idx += 1
                    continue
                ###########

                tmp.append(profile[idx]['line_id'])
                idx += 1

        authorization = {'Authorization': authorization, "Content-Type": "application/json"}
        url = 'https://api.line.me/v2/bot/richmenu/bulk/link'
        for users in users_list:
            if len(users) == 0:
                return True
            logger.debug(users)
            data = {"userIds": users, "richMenuId":richmenu_id}
            response = requests.post(url, data=json.dumps(data), headers=authorization)
            logger.debug(response.text)

            if response.text != '{}':
                return False

        return True

    except Exception as e:
        logger.error(e)
        return False


@api_view(['GET'])
@permission_classes((IsAuthenticated,))
def get_rmlist(request):
    try:
        rmlist = RichmenuList.objects.filter(deleted=False).order_by('id')[::-1]
        data = []
        for rm in rmlist:
            data.append({"label": rm.name,
                         "id": rm.id})


        return JsonResponse({"status": True,
                             "rmlist": data})

    except Exception as e:
        logger.error(e)
        return JsonResponse({"status": False, "msg": ""})
    # try:
    #     invitation_flag = False
    #     try:
    #         inviter_info = BackendSetting.objects.get(name="inviter_info")
    #         info_data = json.loads(inviter_info.data)
    #         if 'line://app' in info_data.get('liff_url'):
    #             invitation_flag = True
    #     except Exception as e:
    #         logger.debug(e)
    #
    #     rmlist = RichmenuList.objects.all().order_by('id')[::-1]
    #     data = []
    #     for rm in rmlist:
    #         data.append({"value": rm.name, "id": rm.id})
    #
    #     return JsonResponse({"status": True, "rmlist": data, "invitation_flag": invitation_flag})
    #
    # except Exception as e:
    #     logger.error(e)
    #     return JsonResponse({"status": False, "invitation_flag": invitation_flag})

@api_view(['GET'])
@permission_classes((IsAuthenticated, ))
def get_richmenu_image(request):
    try:
        image_name = request.GET.get('image_name')

        url = os.environ.get('RM_IMAGE_URL').format(CODE=request.COOKIES['lychee_code'],
                                                    NAME=image_name,
                                                    BUCKET_URL=BUCKET_URL)
        logger.debug('url:')
        logger.debug(url)
        response = requests.get(url)

        if "png" in image_name:
            contentype = "image/png"
        else:
            contentype = "image/jpeg"

        return HttpResponse(response, content_type=contentype)

    except Exception as e:
        logger.error(e)
        return HttpResponseBadRequest()


@api_view(['GET'])
@permission_classes((IsAuthenticated, ))
def get_imagemap_file(request):

    try:
        image_name = request.GET.get('image_name')

        url = os.environ.get('PUSH_URL').format(image="imagemap/"+str(image_name)+"/1040")
        print(url)
        response = requests.get(url)
        return HttpResponse(response)

    except Exception as e:
        logger.error(e)
        return HttpResponseBadRequest()


@api_view(['GET'])
@permission_classes((IsAuthenticated, ))
def get_postback_list(request):
    try:
        tag_events = TrackingEvent.objects.values('name', 'action').filter(type='postback')
        data = []
        for tag_event in tag_events:
            data.append({"value": tag_event['action'],
                         "label": tag_event['name']})

        return JsonResponse({"status": True, "data":data})

    except Exception as e:
        logger.error(e)
        return JsonResponse({"status": False, "msg":str(e)})

# @csrf_exempt
# def rm_liff_app(request):
#     try:
#         if request.method == 'DELETE':
#             body = json.loads(request.body.decode('utf-8'))
#             headers = {"authorization": "Bearer {}".format(body.get('token'))}
#
#             liff_id_list = []
#             success_list = []
#             failure_list = []
#             if body.get('liff_id') is not None:
#                 liff_id_list.append(request.GET['liff_id'])
#             else:
#                 logger.debug('delete all liff')
#                 response = requests.get('https://api.line.me/liff/v1/apps', headers=headers)
#                 if response.status_code >= 400:
#                     return JsonResponse({"status": True, "messages": response.json()})
#
#                 liff_apps = response.json()
#                 for app in liff_apps.get('apps'):
#                     liff_id_list.append(app["liffId"])
#
#             logger.debug('deleting')
#             for liff_id in liff_id_list:
#                 response = requests.delete('https://api.line.me/liff/v1/apps/{}'.format(liff_id), headers=headers)
#                 logger.debug(response.status_code)
#                 if response.status_code == 200:
#                     success_list.append(liff_id)
#                 else:
#                     failure_list.append(liff_id)
#
#             return JsonResponse({"status": True, "success_list": success_list, "failure_list": failure_list})
#
#     except Exception as e:
#         logger.error(e)
#         return JsonResponse({"status": False})

from survey.models import SurveyAccount, Ticket

@api_view(['GET'])
@permission_classes((IsAuthenticated, ))
def geturllist(request):
    try:
        company_code = request.COOKIES['lychee_code']
        events = TrackingEvent.objects.filter(type='uri', deleted=False, created_by=1).order_by('created_at')[::-1]
        now = datetime.utcnow()

        data = []
        for event in events:
            data.append({"id": event.id,
                         "name": event.name,
                         "url": event.data})

        # lucky_draw_events = LuckyDrawEvent.objects.filter(end_date__gte=datetime.utcnow(), deleted=0, show=1).order_by('created_at')[::-1]
        lucky_draw_events = LuckyDrawEvent.objects.filter(deleted=0, show=1).order_by('created_at')[::-1]
        lucky_draw_data = []
        for event in lucky_draw_events:
            end_date = event.end_date
            if end_date is not None:
                is_end = now > event.end_date.replace(tzinfo=None)
            else:
                is_end = False
            lucky_draw_data.append({"id": event.id,
                                    "name": event.name,
                                    "deleted": event.deleted,
                                    "is_end": is_end})

        class_events = ClassificationEvent.objects.filter(deleted=0).order_by('created_at')[::-1]
        logger.debug(class_events)
        class_data = []
        for event in class_events:
            name = event.name
            # if event.type == 'multi':
            #     name = '(多){}'.format(name)

            class_data.append({"id": event.id,
                               "name": name,
                               "type": event.type})

        invitation_flag = False
        setting = BackendSetting.objects.filter(name='inviter_info')
        if len(setting) != 0:
            invitation_flag = True

        member_flag = False
        setting = BackendSetting.objects.filter(name='member_liff_url')
        if len(setting) != 0:
            member_flag = True

        notify_flag = False
        setting = BackendSetting.objects.filter(name='notify_訂閱追蹤')
        if len(setting) != 0:
            notify_flag = True

        loyalty_flag = False
        setting = BackendSetting.objects.filter(name='loyalty_info')
        if len(setting) != 0:
            loyalty_flag = True

        survey_list = []
        tmp = SurveyAccount.objects.filter(deleted=False)
        for s in tmp:
            survey_list.append({'id': s.id,
                                'title': s.title,
                                'start_at': s.start_at,
                                'end_at': s.end_at})

        ticket_list = []
        tmp = Ticket.objects.filter(deleted=False)
        for t in tmp:
            if t.exchange_at is not None:
                is_end = now > t.exchange_end_at.replace(tzinfo=None)
            else:
                is_end = False
            ticket_list.append({'id': t.id,
                                'user_title': t.user_title,
                                'image': os.getenv('BUCKET_URL') + '/ticket/' + company_code + "/" + t.image,
                                "is_end": is_end})

        return JsonResponse({"status": True,
                             "urls": data,
                             "lucky_draw": lucky_draw_data,
                             "classification": class_data,
                             "ticket_list": ticket_list,
                             "invitation_flag": invitation_flag,
                             "member_flag": member_flag,
                             "loyalty_flag": loyalty_flag,
                             "survey_list": survey_list,
                             "notify_flag": notify_flag})

    except Exception as e:
        logger.error(e)
        return JsonResponse({"status": False})

def img_track(request):
    try:
        url = 'https://vignette.wikia.nocookie.net/marvelmovies/images/2/2d/Jarvis_Ultron_Attack_Avengers_Age_of_Ultron.JPG/revision/latest/zoom-crop/width/240/height/240?cb=20151115203054'
        response = requests.get(url)

        image_base64 = BytesIO(response.content)
        image_read = image_base64.read()

        headers = {'Content-Type': 'image/jpg'}

        return {
            "isBase64Encoded": True,
            "status_code": 200,
            "headers": headers,
            "body": image_read.decode("utf-8")
        }

    except Exception as e:
        logger.error(e)

def delete_rm_alias(company_code, code):
    detoken = get_token(company_code)
    headers = {"authorization": "Bearer {}".format(detoken)}
    res = requests.delete(f'https://api.line.me/v2/bot/richmenu/alias/{code}', headers=headers)
    logger.debug(res.status_code)
    return res

def get_rm_all_alias_and_delete(company, linked_rm_list):
    logger.debug('要被刪除的連結 rm_id_list')
    logger.debug(linked_rm_list)
    company_code = company[11:20]

    with connections['default'].cursor() as cur:
        sql = f'''SELECT * FROM {company}.richmenu_items where richmenu_list_id in %s and deleted=0;'''
        logger.debug(sql)
        cur.execute(sql, [linked_rm_list])
        rows = dictfetchall(cur)
        item_id_list = [row['id'] for row in rows]
        logger.debug('要被刪除的連結 item_id_list')
        logger.debug(item_id_list)

        sql = f'''SELECT * FROM {company}.richmenu_alias where richmenu_items_id in %s and status=0;'''
        logger.debug(sql)
        cur.execute(sql, [item_id_list])
        rows = dictfetchall(cur)
        logger.debug('要被刪除的items_id_list')
        logger.debug(rows)

        if len(rows) > 0:
            alias_id_list = []
            for row in rows:
                res = delete_rm_alias(company_code, row['code'])
                if res.status_code == 200:
                    alias_id_list.append(row['id'])
                else:
                    logger.error(f'{company} 刪除alias失敗')
                    logger.error(res.text)

            if len(alias_id_list) > 0:
                sql = f'''UPDATE `{company}`.`richmenu_alias` SET `status` = '1' WHERE (`id` in %s);'''
                cur.execute(sql, [alias_id_list])

@api_view(['POST'])
@permission_classes((IsAuthenticated, ))
def cancel_rich_menu_default(request):
    try:
        data = request.data
        rm_id = int(data.get('rm_id'))
        logger.debug(rm_id)
        code = request.COOKIES['lychee_code']
        company = f'lychee_bot_{code}'
        decode_token = get_token(code)

        with transaction.atomic():
            logger.debug('get_link_rm_dict:')
            rm_dict = get_up_link_rm_dict(company)
            logger.debug(rm_dict)
            logger.debug('get_linked_rm_list:')
            linked_rm_list = get_linked_rm_list(rm_dict, rm_id)
            linked_rm_list.append(rm_id)
            logger.debug("被連結的所有圖文選單:")
            logger.debug(linked_rm_list)

            for rm_id in linked_rm_list:
                rm = RichmenuList.objects.get(id=rm_id)
                status_code = delete_rmId('Bearer {}'.format(decode_token), rm.richmenu_id)
                if status_code == 200:
                    rm.status = 0
                    rm.richmenu_id = None
                    rm.save()

            get_rm_all_alias_and_delete(company, linked_rm_list)

        return JsonResponse({'status': 200, "msg": "成功關閉圖文選單"})
    except Exception as e:
        logger.error(e)
        return JsonResponse({'status': 401, "msg": "發生問題！請洽荔枝智慧!"})

# @api_view(['POST'])
# @permission_classes((IsAuthenticated, ))
# def get_event_hot(request):
#     try:
#         data = request.data
#
#         url_events = []
#         class_events = []
#         lucky_draw_events = []
#
#         total_dict = {"url": {}, "class": {}, "lucky_draw": {}}
#
#         #####################################################################
#         for d in data.get('events'):
#
#             if d.get("type") == "uri":
#                 url_events.append(d.get('content'))
#
#             if d.get("type") == "classification":
#                 class_events.append(d.get('content'))
#
#             if d.get("type") == "lucky_draw":
#                 lucky_draw_events.append(d.get('content'))
#
#         #####################################################################
#         if len(url_events) != 0:
#             event_list = TrackingEvent.objects.filter(id__in=url_events)
#             for row in event_list:
#                 total_dict['uri'][row.get('id')] = row.count
#
#
#         events = data.get('events')
#
#
#
#         counts = []
#         for e in event_list:
#             counts.append(e.count)
#
#
#         return JsonResponse({"status": True,
#                              "counts": counts})
#
#     except Exception as e:
#         logger.error(e)
#         return JsonResponse({"status": False})

def schedule_login():
    payload = {
        "account": os.getenv('SCHEDULE_ACCOUNT'),
        "write_key": os.getenv('SCHEDULE_WRITE_KEY'),
    }
    headers = {
        'Content-Type': "application/json",
    }

    logger.debug("payload:")
    logger.debug(payload)
    # logger.debug("headers:")
    # logger.debug(headers)
    logger.debug("url:")
    logger.debug(os.getenv('SCHEDULE_HOST')+'/login')

    res = requests.post(os.getenv('SCHEDULE_HOST')+'/login', json.dumps(payload), headers=headers)
    logger.debug("res:")
    logger.debug(res.text)

    if res.status_code == 200:
        return json.loads(res.text)['jwt_token']
    else:
        return None

def schedule_create(date='202107200247', callback='https://2684b1fdd263.ngrok.io', method='POST', data={"user_id": "user123321", "message": "hehexdxdxdxd"}):
    token = cache.get('schedule_token')
    if token is None:
        token = schedule_login()
        if token is None:
            return None
        cache.set('schedule_token', token, timeout=10*60)

    payload = {
        "target_info": {
            "date_time": date,
            "callback": callback,
            "method": method
        },
        "data": data
    }
    headers = {
        'jwt_token': token,
        'Content-Type': "application/json",
    }
    logger.debug("payload:")
    logger.debug(payload)
    # logger.debug("headers:")
    # logger.debug(headers)
    res = requests.post(os.getenv('SCHEDULE_HOST')+'/events', json.dumps(payload), headers=headers)
    return res.status_code

## 排程推播
# @api_view(['POST'])
# @permission_classes((IsAuthenticated, ))
# def schedule_link(request):
#     data = request.data
#     print('=======================')
#     print(data)
#     print('=======================')
    # return JsonResponse({"status": 200})
