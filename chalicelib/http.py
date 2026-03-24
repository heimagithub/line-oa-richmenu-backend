from chalice import Response


def ok(data, status_code=200):
    return {"data": data} if status_code != 204 else Response(status_code=204, body="")


def success(data=None):
    body = {"success": True}
    if data is not None:
        body["data"] = data
    return body


def error(code: str, message: str, status_code: int = 400, details=None):
    payload = {"error": {"code": code, "message": message}}
    if details is not None:
        payload["error"]["details"] = details
    return Response(status_code=status_code, body=payload)
