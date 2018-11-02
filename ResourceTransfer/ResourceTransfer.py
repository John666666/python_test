#!/usr/bin/python
# -*-coding:utf8-*-
import ConfigParser
import logging
from logging import config
import pymysql
from pymysql.cursors import DictCursor
import time
from QiniuHelper import *

## 初始化日志
config.fileConfig("./logging.conf")
logger = logging.getLogger("my_logger")

## 读取配置
config = ConfigParser.SafeConfigParser()
config.read('./conf.ini')

data_file_path = "data.xlsx"

class ResourceTransfer():
    '''
        老资源迁移到新的Resource库中
    '''
    def __init__(self):
        self.__connectDB()
        self.labelMap = self.__loadLabels()
        self.subCategoryMap = self.__loadSubCategories()
        self.categoryMap = self.__loadCategories()

    def __loadLabels(self):
        labelMap = {}
        self.resource_cursor.execute("select id,name from res_tag where parent_id is not null")
        for label in self.resource_cursor.fetchall():
            labelMap[label['name']] = label['id']
        return labelMap

    def __loadSubCategories(self):
        subCategoryMap = {}
        self.resource_cursor.execute("select id, category_id, name from res_category_sub")
        for subCategory in self.resource_cursor.fetchall():
            if not subCategoryMap.has_key(subCategory['name']):
                subCategoryMap[subCategory['name']] = {}    # {子分类: {categoryId1: subCategoryId1, categoryId2: subCategoryId2}}
            subCategoryObj = subCategoryMap[subCategory['name']]
            subCategoryObj[subCategory['category_id']] = subCategory['id']
        return subCategoryMap

    def __loadCategories(self):
        categoryMap = {}
        self.resource_cursor.execute("select id, name from res_category")
        for category in self.resource_cursor.fetchall():
            categoryMap[category['name']] = category['id']
        return categoryMap


    def __loadDataFromExcel(self, filePath):
        dataList = []
        fieldNames = ['albumId', 'albumName', 'categoryName', 'subCategoryName', 'tagName']
        import xlrd
        try:
            wb = xlrd.open_workbook(filePath)
            sheet = wb.sheet_by_index(0)
        except Exception, e:
            logger.error(u"解析excel出错: %s", str(e))
            return

        nrows = sheet.nrows
        ncols = sheet.ncols

        for rownum in range(1, nrows):
            row = sheet.row_values(rownum)
            rowItem = {}
            for colnum in range(0, ncols):
                cellData = row[colnum]
                if isinstance(cellData, unicode):
                    cellData = cellData.lstrip().rstrip()
                rowItem[fieldNames[colnum]] = cellData
            if not rowItem:
                logger.warning(u"跳过空行, 行号: %d", rownum)
                continue
            dataList.append(rowItem)
        return dataList

    def __getQiniuImageUrlByImageId(self, qiniuImageId):
        if not qiniuImageId:
            return None
        self.lb_cursor.execute("select QINIU_KEY, BUCKET from qiniu_image_res where id = %s" % (qiniuImageId, ))
        qiniuImage = self.lb_cursor.fetchone()
        if not qiniuImage:
            logger.warning(u"指定的七牛图片ID在图片库中不存在, id: %s" , qiniuImageId)
            return None
        qiniuKey = qiniuImage["QINIU_KEY"]
        bucket = qiniuImage["BUCKET"]
        return getQiniuResourceUrlByQiniuKeyAndBucket(qiniuKey, bucket)

    def __loadOldMediaByAlbumId(self, albumId):
        self.lb_cursor.execute("select * from x2_media where album_id = %s", (albumId, ))
        return self.lb_cursor.fetchall()

    def __loadOldAlbumInfoByAlbumId(self, albumId):
        self.lb_cursor.execute("select * from x2_album where id = %s", (albumId, ))
        return self.lb_cursor.fetchone()

    def __getSingleCountByAlbumId(self, albumId):
        self.lb_cursor.execute("select count(*) as single_count from x2_media where album_id = %s", (albumId, ))
        result = self.lb_cursor.fetchone()
        if not result:
            return None
        return result["single_count"]


    def __supplementIdInfos(self, transferData):
        if not transferData or len(transferData) < 1:
            logger.warning(u"没有读取到配置数据，请检查配置文件")
            return
        # ['albumId', 'albumName', 'categoryName', 'subCategoryName', 'tagName']
        for data in transferData:
            logger.debug(u"process: %s", data)
            try:
                categoryId = self.categoryMap[data["categoryName"]]
                subCategoryId = self.subCategoryMap[data["subCategoryName"]][categoryId]
                tagId = self.labelMap[data["tagName"]]
            except Exception, e:
                logger.error(u"分类或子分类或标签在resource库中不存在：%s", str(e))
                continue
            if not categoryId or not subCategoryId or not tagId:
                logger.warning(u"指定的分类名称或子分类名称或标签名称在resource库中不存在: %s, 请检查" % (data, ))
                transferData.remove(data)   # TODO 遍历中删除元素，有可能会有问题
                continue
            data["categoryId"] = categoryId
            data["subCategoryId"] = subCategoryId
            data["tagId"] = tagId

    def __supplementAlbumOtherFields(self, transferData):
        if not transferData or len(transferData) < 1:
            logger.warning(u"没有读取到配置数据，请检查配置文件")
            return
        # ['albumId', 'albumName', 'categoryName', 'subCategoryName', 'tagName']
        for data in transferData:
            logger.debug("process: %s" % data)
            albumId = data["albumId"]
            albumInfo = self.__loadOldAlbumInfoByAlbumId(albumId)
            if not albumInfo:
                logger.warning(u"专辑信息不存在, id: %s", albumId)
                continue
            qiniuImageId = albumInfo["IMG_ID_MID"]
            imageUrl = self.__getQiniuImageUrlByImageId(qiniuImageId)
            description = albumInfo["DESCRIPTION"]
            sourceId = 1  #设置为自有资源
            lang = albumInfo["LANG"]
            singleCount = self.__getSingleCountByAlbumId(albumId)
            sort = 1000  #将专辑排序都设置为1000
            fitDevice = 0   #设配所有设备
            status = 0  #先设置为禁用(测过后改成启用：1)
            createtime = albumInfo["CREATE_TIME"]

            data["image_url"] = imageUrl
            data["description"] = description
            data["source_id"] = sourceId
            data["lang"] = lang
            data["single_count"] = singleCount
            data["sort"] = sort
            data["fit_device"] = fitDevice
            data["status"] = status
            data["create_time"] = createtime


    def transfer(self):
        u"""
            数据迁移入口方法，负责将配置文件中需要迁移的老资源专辑迁移到新resource库中
        :return:
        """
        # 1. load data from excel
        transferData = self.__loadDataFromExcel(data_file_path)
        logger.info(u"加载配置成功, 需要迁移的专辑数量：%d", len(transferData))

        # 2. 根据配置中的大类、子类、标签名称补充对应的大类ID、子类ID、标签ID
        self.__supplementIdInfos(transferData)
        logger.info(u"补充专辑对应分类、标签信息的ID完成, 专辑数量：%d", len(transferData))

        # 3. 补充专辑信息缺失的字段
        self.__supplementAlbumOtherFields(transferData)
        logger.info(u"补充专辑信息其他字段完成, 专辑数量：%d", len(transferData))

        for dataItem in transferData:

            self.resource_conn.begin()
            try:
                # 4. 插入专辑信息到resource库中
                self.resource_cursor.execute("insert into res_album(category_id, category_sub_id, name, image_url, "
                                             "description, source_id, lang, single_count, sort, fit_device, status, "
                                             "create_time, update_time) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())",
                                             (dataItem["categoryId"], dataItem["subCategoryId"], dataItem["albumName"],
                                              dataItem["image_url"], dataItem["description"], dataItem["source_id"], dataItem["lang"],
                                              dataItem["single_count"], dataItem["sort"], dataItem["fit_device"], dataItem["status"],
                                              dataItem["create_time"]))
                newAlbumId = self.resource_conn.insert_id()
                if not newAlbumId:
                    raise Exception(u"无法获取专辑自增ID")
                logger.info(u"保存专辑%s信息成功", dataItem["albumName"])

                # 5. 插入专辑-标签关联信息到resource库中
                self.resource_cursor.execute("insert into res_album_tag_relation (album_id, tag_id, create_time, update_time)"
                                             " values (%s, %s, now(), now())",
                                             (newAlbumId, dataItem["tagId"]))
                logger.info(u"保存专辑%s对应的标签信息成功", dataItem["albumName"])

                # 6. 循环加载每个专辑下的单曲，补充缺失的字段，并插入到resource库中
                oldAlbumId = dataItem["albumId"]
                mediaList = self.__loadOldMediaByAlbumId(oldAlbumId)
                if not mediaList or len(mediaList) < 1:
                    logger.warning(u"%s专辑下没有单曲", dataItem["albumName"])
                    continue
                paramList = []
                for mediaItem in mediaList:
                    name = mediaItem["NAME"]
                    mediaType = 1
                    if mediaItem["MEDIA_TYPE"] == "video":
                        mediaType = 2
                    author = mediaItem["AUTHOR"]
                    playUrl = getQiniuResourceUrlByQiniuKeyAndBucket(mediaItem["QINIU_KEY"], mediaItem["BUCKET"])
                    sort = mediaItem["IDX"]
                    sourceId = 1
                    imageUrl = self.__getQiniuImageUrlByImageId(mediaItem["IMG_ID_MID"])
                    status = 0
                    fitDevice = 0
                    playState = 1
                    createtime = mediaItem["CREATE_TIME"]
                    paramList.append((newAlbumId, mediaType, name, author, playUrl, sort, sourceId, imageUrl, status,
                                      fitDevice, playState, createtime, ))
                self.resource_cursor.executemany("insert into res_media(album_id, media_type, name, author, play_url, sort, "
                                             "source_id, image_url, status, fit_device, play_state, create_time, update_time)"
                                             " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())", paramList)
                effectRowNum = self.resource_cursor.rowcount
                logger.info(u"%s专辑插入%s单曲. ", dataItem["albumName"], effectRowNum)
                self.resource_conn.commit()
            except Exception, e:
                logger.error(u"保存资源到resource库失败, %s", str(e))
                self.resource_conn.rollback()
                return 1


    def __connectDB(self):
        self.resource_conn = pymysql.connect(host=config.get('db', 'resource.host'),
                                    user=config.get('db', 'resource.user'),
                                    passwd=config.get('db', 'resource.passwd'),
                                    db=config.get('db', 'resource.db'),
                                    port=config.getint('db', 'resource.port'),
                                    charset=config.get('db', 'resource.charset'))
        self.resource_cursor = self.resource_conn.cursor(DictCursor)

        self.lb_conn = pymysql.connect(host=config.get('db', 'lb2.host'),
                                       user=config.get('db', 'lb2.user'),
                                       passwd=config.get('db', 'lb2.passwd'),
                                       db=config.get('db', 'lb2.db'),
                                       port=config.getint('db', 'lb2.port'),
                                       charset=config.get('db', 'lb2.charset'))
        self.lb_cursor = self.lb_conn.cursor(DictCursor)

    def closeDB(self):
        try:
            self.resource_cursor.close()
        except:
            pass
        try:
            self.resource_conn.close()
        except:
            pass
        try:
            self.lb_cursor.close()
        except:
            pass
        try:
            self.lb_conn.close()
        except:
            pass

if __name__ == '__main__':
    transfer = ResourceTransfer()
    transfer.transfer()
    transfer.closeDB()
