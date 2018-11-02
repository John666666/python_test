#!/usr/bin/python
# -*-coding:utf8-*-
from qiniu import Auth
access_key = ''
secret_key = ''

bucketDomain = {
    "luobotec-image": "img.luobotec.com",
    "lubotec-video": "video.luobotec.com"
}

publicBucket = ["lubotec-video", "luobotec-image"]

def getQiniuResourceUrlByQiniuKeyAndBucket(qiniuKey, bucket):
    if not bucketDomain. has_key(bucket):
        raise Exception(u"不支持的bucket: %s" % (bucket, ))
    domain = bucketDomain.get(bucket)
    q = Auth(access_key, secret_key)
    base_url = 'http://%s/%s' % (domain, qiniuKey, )
    if bucket in publicBucket:
        #公开空间，不需要token
        return base_url
    #可以设置token过期时间
    private_url = q.private_download_url(base_url, expires=0)
    return private_url

if __name__ == '__main__':
    print getQiniuResourceUrlByQiniuKeyAndBucket("粉红猪小妹_mid.png", "luobotec-image")
    print getQiniuResourceUrlByQiniuKeyAndBucket("3_悯农.mp3", "lubotec-video")