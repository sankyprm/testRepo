__author__ = 'soumik'
import datetime
import urllib
import hmac
import hashlib
import base64
AWSAccessKeyId = "AKIAJDLBFRK3LEIEENZQ"
secret = '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'
SignatureVersion = "2"
SignatureMethod = "HmacSHA256"
ServiceHost = "awis.amazonaws.com"
PATH = "/"

def AlexaSettings(Action,Url,ResponseGroup):

    def create_timestamp():
        now = datetime.datetime.now()
        timestamp = now.isoformat()
        return timestamp

    def create_uri(params):
        params = [(key, params[key])
            for key in sorted(params.keys())]
        return urllib.urlencode(params)

    def create_signature():
        Uri = create_uri(params)
        msg = "\n".join(["GET", ServiceHost, PATH, Uri])
        hmac_signature = hmac.new(secret.encode(), msg.encode(), hashlib.sha256)
        signature = base64.b64encode(hmac_signature.digest())
        return urllib.quote(signature)

    params = {
    'Action':Action,
    'Url':Url,
    'ResponseGroup':ResponseGroup,
    'SignatureVersion':SignatureVersion,
    'SignatureMethod':SignatureMethod,
    'Timestamp': create_timestamp(),
    'AWSAccessKeyId':AWSAccessKeyId,
    #'Path' : 'http://stackoverflow.com/jobs'
            }

    uri = create_uri(params)
    signature = create_signature()
    url = "http://%s/?%s&Signature=%s" % (ServiceHost, uri, signature)
    return url
