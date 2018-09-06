#!/usr/bin/python
# -*- coding: utf-8 -*-
import re

# import MySQLdb
import mysql.connector as mysqlHelper
from datetime import datetime

numberPattern = r"^\d+$"
charPrefixPattern = r"^([a-zA-Z]+[\d]+[a-zA-Z]+)(\d+)$"
numberReg = re.compile(numberPattern)
charPrefixReg = re.compile(charPrefixPattern)

DEVICE_TYPE_NUMBER = 1
DEVICE_TYPE_CHARPREFIX = 2


class BatchAuthorization:

    def __init__(self, start, productType, robotType, size, batchSize=100, isProduction=True):
        self.start = start
        self.productType = productType
        self.robotType = robotType
        self.size = size
        self.batchSize = batchSize
        self.isProduction = isProduction
        self.__initDBConnection()

    def __initDBConnection(self):
        self.conn = mysqlHelper.connect(user='root',
                                        password='root123',
                                        host='127.0.0.1',
                                        database='lb2api')

    def startAuthorization(self):
        deviceIds = self.generateDeviceIDsBySequence()
        if not deviceIds:
            print u"无法自动生成要授权的设备号列表"
            return
        isSuccess = self.authorizationAccount(deviceIds)
        if isSuccess:
            isSuccess = self.authorizationChuantou(deviceIds)
        else:
            print u"授权失败"
            return
        if isSuccess:
            print u"请根据生成的穿透授权配置文件手动完成授权"
        else:
            print u"穿透授权配置文件生成失败"



    def authorizationAccount(self, deviceIds):
        """
        设备ID后台授权
        :param deviceIds:设备号列表
        :return 授权成功返回True，否则返回False
        """
        self.conn.start_transaction()
        cursor = self.conn.cursor()
        sql = "insert into robot (robot_type ,imei, product_type, status, pdate, activate_date, create_time, update_time) " \
              "values (%s, %s, %s, 0, now(), now(), now(), now());"
        batchParams = []
        try:
            for deviceId in deviceIds:
                batchParams.append((self.robotType, deviceId, self.productType, ))
                if len(batchParams) % self.batchSize == 0:
                    rowCount = cursor.executemany(sql, batchParams)
                    del batchParams[:]
                    print u"批量授权: %s 条." % cursor.rowcount
            if len(batchParams) > 0:
                rowCount = cursor.executemany(sql, batchParams)
                del batchParams[:]
                print u"批量授权: %s 条." % cursor.rowcount
        except Exception, e:
            self.conn.rollback()
            print u"账号插入数据库失败", e
            return False
        else:
            self.conn.commit()
            return True
        finally:
            cursor.close()
            self.conn.close()

    def authorizationChuantou(self, deviceIds):
        """
        穿透授权
        :param deviceIds: 设备号列表
        :return:
        """
        dt = datetime.now().strftime("%Y%m%d")
        fileName = "user-list-%s-%s-%s.txt" % (self.productType, "product" if self.isProduction else "dubug", dt)
        try:
            confFile = open(fileName, "a", )
            for deviceId in deviceIds:
                confFile.write("_CAP_%s 1234\n" % deviceId)
            confFile.close()
        except Exception,e:
            print u"生成穿透授权配置文件失败", e
            return False
        else:
            return True


    def generateDeviceIDsBySequence(self):
        """
        枚举出本批要授权的设备ID
        :param start: 开始号段
        :param size: 生成数量
        :return:
        """
        start = self.start
        size = self.size
        deviceList = []
        orginLen = len(start)
        print u"设备号长度：%d" % orginLen
        prefix = ""
        startIdx = None
        deviceType = None
        if numberReg.match(start):
            deviceType = DEVICE_TYPE_NUMBER
            prefix = ""
            startIdx = numberReg.findall(start)[0]
        elif charPrefixReg.match(start):
            deviceType = DEVICE_TYPE_CHARPREFIX
            matchGroup = charPrefixReg.findall(start)[0]
            print matchGroup
            prefix = matchGroup[0]
            startIdx = matchGroup[1]

        print u"设备号前缀：%s, 开始序号：%s" % (prefix, startIdx)

        for i in range(size):
            idx = int(startIdx) + i
            deviceId = self.__paddingDeviceId(prefix, orginLen, idx, deviceType)
            if len(deviceId) > orginLen:
                print u"可用号段不足"
                break
            else:
                print deviceId
                deviceList.append(deviceId)
        return deviceList

    def __paddingDeviceId(self, prefix, length, idx, type):
        if not type:
            type = DEVICE_TYPE_CHARPREFIX

        if type == DEVICE_TYPE_NUMBER:
            return ("%s%s" % (prefix, idx)).zfill(length)
        elif type == DEVICE_TYPE_CHARPREFIX:
            paddingLen = length - len(prefix)
            return "%s%s" % (prefix, ("%d" % idx).zfill(paddingLen))
        else:
            print u"无法处理的设备类型"
            return None


if __name__ == '__main__':
    authorTool = BatchAuthorization(start="DYLBL2PBJAHJC0001", size=10, productType="2p", robotType="1")
    authorTool.startAuthorization()
