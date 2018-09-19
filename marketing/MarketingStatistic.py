#!/usr/bin/python
# -*-coding:utf8-*-
import ConfigParser
import logging
import logging.config
from datetime import datetime, timedelta
import json
import urllib
import base64

import requests
import MySQLdb
from MySQLdb.cursors import DictCursor
from apscheduler.schedulers.blocking import BlockingScheduler

__author__ = 'John'

## 初始化日志
logging.config.fileConfig("./logging.conf")
logger = logging.getLogger("my_logger")

## 读取配置
config = ConfigParser.SafeConfigParser()
config.read('./conf.ini')

# marketing_type = "spring"
# marketing_type = "christmas"
marketing_type = "Couplet"

def getStrTime(format='%Y-%m-%d %H:%M:%S', dt=None):
    '''
        按指定格式格式化当前时间
    :param format: 时间格式
    :return:
    '''
    if dt is None:
        dt = datetime.now()
    return dt.strftime(format)


class Christmas():
    '''
        圣诞节运营活动统计相关
    '''
    def __init__(self):

        self.jpush_url = config.get('jpush', 'url')
        self.jpush_appKey = config.get('jpush', 'appKey')
        self.jpush_masterSecret = config.get('jpush', 'masterSecret')

    def __connectDB(self):
        self.conn = MySQLdb.connect(host=config.get('db', 'nlu.host'),
                                    user=config.get('db', 'nlu.user'),
                                    passwd=config.get('db', 'nlu.passwd'),
                                    db=config.get('db', 'nlu.db'),
                                    port=config.getint('db', 'nlu.port'),
                                    charset=config.get('db', 'nlu.charset'))
        self.cursor = self.conn.cursor(DictCursor)

        self.lb_conn = MySQLdb.connect(host=config.get('db', 'lb2.host'),
                                       user=config.get('db', 'lb2.user'),
                                       passwd=config.get('db', 'lb2.passwd'),
                                       db=config.get('db', 'lb2.db'),
                                       port=config.getint('db', 'lb2.port'),
                                       charset=config.get('db', 'lb2.charset'))
        self.lb_cursor = self.lb_conn.cursor(DictCursor)

    def __closeDB(self):
        try:
            self.cursor.close()
        except:
            pass
        try:
            self.conn.close()
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

    def statistic(self):
        '''
            统计截止当前， 当天活动参与情况。 可定时调用
        :return:
        '''
        startTime = '2018-3-15 00:00:00'
        # endTime = getStrTime()
        # endTime = '2017-12-24 21:00:00'

        self.__connectDB()

        sql = """select user_id as imei, req_content as question, params as answer from intent_analysis_log where domain = 'alga'
            and intent = 'couplet' and req_time >= %s"""
        self.cursor.execute(sql, (startTime, ))
        rows = self.cursor.fetchall()
        total_map = {}   #总参与次数
        success_map = {} #成功参与次数
        rd_map = {}      #有效参与次数
        for row in rows:
            answer_json = json.loads(row["answer"])
            answer = answer_json['speechText']
            imei = row['imei']
            if not total_map.has_key(imei):
                total_map[imei] = []
            if answer.startswith(u'恭喜你，回答正确') or answer.startswith(u'答错了'):
                total_map.get(imei).append(answer)
            if answer.startswith(u'恭喜你，回答正确'):
                if not success_map.has_key(imei):
                    success_map[imei] = []
                success_map.get(imei).append(answer)


        for key in success_map:
            rd_map[key] = list(set(success_map.get(key)))


        #print json.dumps(total_map, ensure_ascii=False)
        #print json.dumps(success_map, ensure_ascii=False)
        #print json.dumps(rd_map, ensure_ascii=False)

        job_no = getStrTime(format='%Y%m%d%H%M%S')

        for key in total_map.keys():
            total_count = len(total_map.get(key))
            success_count = 0
            rd_count = 0
            if success_map.has_key(key):
                success_count = len(success_map.get(key))
            if rd_map.has_key(key):
                rd_count = len(rd_map.get(key))

            sql = """insert into marketing_statistic (job_no, marketing_type, statistic_type, robot_imei, total_count, success_count, rd_count, createtime)
                  values (%s, %s, %s, %s, %s, %s, %s, now())"""

            try:
                self.cursor.execute(sql, (job_no, marketing_type, "detail", key, total_count, success_count, rd_count))
                self.conn.commit()
            except Exception, e:
                # logger.error("插入统计结果出错", e)
                logger.error("插入统计结果出错")

        self.__closeDB()

    def day_report(self):
        '''
            每天发送给用户的通知
        :return:
        '''
        self.__connectDB()

        sql = "select job_no FROM marketing_statistic where marketing_type = %s order by job_no desc limit 1"
        self.cursor.execute(sql, (marketing_type, ))
        row = self.cursor.fetchone()
        if row is None:
            logger.error("没有查询到统计数据， 无法发送日报")
            return
        job_no = row["job_no"]
        print row

        sql = "select job_no, marketing_type, statistic_type,robot_imei, total_count, success_count, rd_count " \
              "FROM marketing_statistic WHERE job_no = %s ORDER BY rd_count DESC"
        self.cursor.execute(sql, (job_no, ))
        rows = self.cursor.fetchall()

        if rows is None:
            logger.error("没有查询到指定批次[%s]统计数据， 无法发送日报" % job_no)
            return

        ## 春节活动统计
        # 参与人数
        # peple_count = len(rows)
        # self.push_msg(u"已经有超过%d人参与春节民俗知多少活动啦!" % (peple_count + 200), alias=['18601245428'])


        ## 圣诞活动统计
        # 生成排行榜
        score_map = {}
        level = 1  #第一名
        last = None  #上一次分数， 如果本次比这个分数小， 则排名++
        for row in rows:
            if last is None:
                score_map["%d" % level] = [row]
                last = row
                continue
            last_score = last["rd_count"]
            current = row["rd_count"]
            if current < last_score:
                level += 1
                score_map["%d" % level] = [row]
            elif current == last_score:
                score_map["%d" % level].append(row)
            elif current > last_score:
                logger.warning("数据库返回结果集排序不正确， 请检查SQL语句是否有误!")
                return
            last = row

        man_count = 0

        # 发送通知
        for level in sorted(score_map.keys(), cmp=lambda x, y: cmp(int(x), int(y))):
            logger.debug("名次: %s" % level)
            robot_list = score_map.get(level)
            print "排名: %s" % level
            if robot_list is None:
                continue

            for robot in robot_list:
                total_count = robot["total_count"]
                success_count = robot["success_count"]
                rd_count = robot["rd_count"]

                #if int(level) >= 8:
                #    message = u"最后一天倒计时！目前您总共解锁了%d个圣诞元素，成绩名列前茅，恭喜！" % rd_count
                #else:
                #    message = u"最后一天倒计时！目前您总共解锁了%d个圣诞元素，还有机会，不要放弃！" % rd_count

                imei = robot["robot_imei"]
                sql = "SELECT uacc,ualias,role from robot_user WHERE RACC = %s and STATUS = 0"
                self.lb_cursor.execute(sql, (imei, ))
                rows = self.lb_cursor.fetchall()
                if rows is None:
                    continue
                man_count += 1
                for row in rows:
                    phone = row["uacc"]
                    userName = row["ualias"]
                    role = row["role"]
                    print u"用户:%s\n手机号:%s\n角色:%s\n总参与次数:%d\n解锁次数:%d\n有效次数:%d\n" % (userName, phone, role, total_count, success_count, rd_count)
                    if man_count > 17:
                        message = u"圣诞寻宝活动圆满结束啦～这次没中奖的小主人下次要努力了哦～"
                    else:
                        message = u"恭喜你在寻宝游戏里中奖了！我们的客服会在3-5个工作日内和您联系，请保持联系方式畅通～"
                    # self.push_msg(message, alias=[phone])
                print u"================================================"
                if man_count > 17:
                    print u"(前17)榜外用户----------------------------------------------------------------"

        self.__closeDB()


    def notifyAllUser(self):
        message = u"圣诞寻宝活动圆满结束啦～这次没中奖的小主人下次要努力了哦～"
        # message = u"圣诞寻宝活动结束啦！快来看看你有没有中奖呢？"
        self.push_msg(message)


    def finish_report(self):
        '''
            活动结束，公布最终中奖名单，通知用户
        :return:
        '''
        self.__connectDB()

        sql = "select job_no FROM marketing_statistic where marketing_type = %s order by job_no desc limit 1"
        self.cursor.execute(sql, (marketing_type, ))
        row = self.cursor.fetchone()
        if row is None:
            logger.error("没有查询到统计数据， 无法发送日报")
            return
        job_no = row["job_no"]
        print row

        sql = "select job_no, marketing_type, statistic_type,robot_imei, total_count, success_count, rd_count " \
              "FROM marketing_statistic WHERE job_no = %s ORDER BY rd_count DESC"
        self.cursor.execute(sql, (job_no, ))
        rows = self.cursor.fetchall()

        if rows is None:
            logger.error("没有查询到指定批次[%s]统计数据， 无法发送日报" % job_no)
            return

        # 生成排行榜
        score_map = {}
        level = 1  #第一名
        last = None  #上一次分数， 如果本次比这个分数小， 则排名++
        for row in rows:
            if last is None:
                score_map["%d" % level] = [row]
                last = row
                continue
            last_score = last["rd_count"]
            current = row["rd_count"]
            if current < last_score:
                level += 1
                score_map["%d" % level] = [row]
            elif current == last_score:
                score_map["%d" % level].append(row)
            elif current > last_score:
                logger.warning("数据库返回结果集排序不正确， 请检查SQL语句是否有误!")
                return
            last = row

        man_count = 0

        # 发送通知
        for level in sorted(score_map.keys(), cmp=lambda x, y: cmp(int(x), int(y))):
            logger.debug("名次: %s" % level)
            robot_list = score_map.get(level)
            print "排名: %s" % level
            if robot_list is None:
                continue

            for robot in robot_list:
                total_count = robot["total_count"]
                success_count = robot["success_count"]
                rd_count = robot["rd_count"]

                #if int(level) >= 8:
                #    message = u"最后一天倒计时！目前您总共解锁了%d个圣诞元素，成绩名列前茅，恭喜！" % rd_count
                #else:
                #    message = u"最后一天倒计时！目前您总共解锁了%d个圣诞元素，还有机会，不要放弃！" % rd_count

                imei = robot["robot_imei"]
                sql = "SELECT uacc,ualias,role from robot_user WHERE RACC = %s and STATUS = 0"
                self.lb_cursor.execute(sql, (imei, ))
                rows = self.lb_cursor.fetchall()
                if rows is None:
                    continue
                man_count += 1
                for row in rows:
                    phone = row["uacc"]
                    userName = row["ualias"]
                    role = row["role"]
                    print u"用户:%s\n手机号:%s\n角色:%s\n总参与次数:%d\n解锁次数:%d\n有效次数:%d\n" % (userName, phone, role, total_count, success_count, rd_count)
                    if man_count > 17:
                        message = u"圣诞寻宝活动圆满结束啦～这次没中奖的小主人下次要努力了哦～"
                    else:
                        message = u"恭喜你在寻宝游戏里中奖了！我们的客服会在3-5个工作日内和您联系，请保持联系方式畅通～"
                    #self.push_msg(message, alias=[phone])
                print u"================================================"
                if man_count > 17:
                    print u"(前17)榜外用户----------------------------------------------------------------"

    def send_app_mail(self, id):
        if not id:
            return
        self.__connectDB()
        sql = "select * from app_prompt WHERE id = %s"
        self.lb_cursor.execute(sql, (id, ))
        row = self.lb_cursor.fetchone()
        if row is None:
            logger.warning(u"邮件ID不存在，请先在邮件表中配置数据")
            return
        message = row["title"]
        sql = "insert into jpush_log (correlation_id, type, tags, create_time) values (%s, %s, %s, %s)"
        n = self.lb_cursor.execute(sql, (id, "prompt", "android,ios", getStrTime(dt=datetime.now())))
        self.lb_conn.commit()
        if n > 0:
            message = u"对句赢好礼，小主人参加了嘛？"
            self.push_msg(message)

        self.__closeDB()

    def push_msg(self, message, alias=None, tags=None):
        '''
            极光推送
        :return:
        '''

        if message is None:
            logger.warning("要推送的内容不能为空!")
            return

        if not isinstance(message, unicode):
            logger.error("只能发送unicode编码字符!")
            return

        params = {}
        params["platform"] = ["android", "ios"]
        params["audience"] = {}
        if alias is not None:
            params["audience"]["alias"] = alias
        if tags is not None:
            params["audience"]["tag"] = []
        if alias is None and tags is None:
            params["audience"] = "all"

        params["notification"] = {
            "android": {
                "alert": message,
                "title": u"《春节民俗知多少》战报",
                "priority": 2
            },
            "ios": {
                "alert": message,
                "badge": 1,
            }
        }
        print json.dumps(params, ensure_ascii=False)
        sign = base64.encodestring("%s:%s" % (self.jpush_appKey, self.jpush_masterSecret)).lstrip().rstrip()
        sign = "Basic %s" % sign
        headers = {'Content-Type': 'application/json', 'Authorization': sign}
        resp = requests.post(self.jpush_url, data=urllib.urlencode(params), headers=headers)
        if resp.status_code == 200:
            print resp.content
        else:
            print "请求出错: %s \b%s" % (resp.status_code, resp.content)

    def schedule_test(self):
        time_str = getStrTime()
        logger.info("schedule job start: %s" % time_str)
        logger.info("schedule job end: %s" % datetime.today())

    def run_job(self):
        schedule = BlockingScheduler()
        schedule.add_job(self.day_job, 'cron', hour=23, minute=59, second=59)
        # schedule.add_job(self.day_job, 'cron', second=10)
        # schedule.add_job(christmas.schedule_test, 'cron', hour=11, minute=9)
        schedule.start()

    def day_job(self):
        logger.info("开始统计")
        self.statistic()
        # logger.info("开始通知参与用户")
        # self.day_report()
        # logger.info("开始通知所有用户")
        #self.notifyAllUser()

if __name__ == '__main__':
    christmas = Christmas()
    # christmas.push_msg(u"测试消息", alias=["18601245428", "18796841347"])

    # 测试
    # christmas.day_job()
    # christmas.finish_report()
    # christmas.send_app_mail(8)
    # christmas.statistic()
    # christmas.day_report()
    # 定时任务
    christmas.run_job()


