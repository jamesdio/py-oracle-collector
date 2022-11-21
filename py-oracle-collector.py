# -*- coding: utf-8 -*-
import config
from time import time
from flask import Flask, request, current_app as app
from datetime import datetime
from influxdb import InfluxDBClient 
from influxdb.exceptions import InfluxDBClientError
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import socket
import subprocess
import re
import cx_Oracle
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json

# for logging
logging.basicConfig()
log = logging.getLogger('apscheduler.executors.default')
log.setLevel(logging.WARNING)  # DEBUG

fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(fmt)
log.addHandler(h)

fqdn = socket.getfqdn()

class OraStats():

    def __init__(self, user, passwd, sid, timestamp):
        self.user = user
        self.passwd = passwd
        self.sid = sid
        self.timestamp = timestamp
        self.delengine = "none"
        self.connection = cx_Oracle.connect(self.user, self.passwd, self.sid)
        with self.connection.cursor() as cursor:
            cursor.execute("select distinct(SVRNAME)  from v$dnfs_servers")
            rows = cursor.fetchall()

            for i in range(0, cursor.rowcount):
                self.dengine_ip = rows[i][0]
                proc = subprocess.Popen(["nslookup", self.dengine_ip], stdout=subprocess.PIPE)
                lookupresult = proc.communicate()[0].split('\n')

                for line in lookupresult:
                    if 'name=' in re.sub(r'\s', '', line):
                        self.delengine = re.sub('\..*$', '', re.sub(r'^.*name=', '', re.sub(r'\s', '', re.sub(r'.$', '', line))))

    ## Interval 로 수행되는 Batch 정의
    
    def waitclassstats(self, data):
        with self.connection.cursor() as cursor:
            cursor.execute("""
            select n.wait_class
                   ,round(m.time_waited/m.INTSIZE_CSEC,3) AAS
            from   v$waitclassmetric  m, 
                   v$system_wait_class n
            where  m.wait_class_id=n.wait_class_id 
            and    n.wait_class != 'Idle'
            union
            select  'CPU'
                    ,round(value/100,3) AAS
            from    v$sysmetric 
            where   metric_name='CPU Usage Per Sec' 
            and     group_id=2
            union  
            select 'CPU_OS'
                   ,round((prcnt.busy*parameter.cpu_count)/100,3) - aas.cpu
            from
                    ( 
                      select value busy
                      from   v$sysmetric
                      where  metric_name = 'Host CPU Utilization (%)'
                        and  group_id=2 
                    ) prcnt,
                    ( 
                      select value cpu_count 
                      from   v$parameter 
                      where  name = 'cpu_count' 
                    )  parameter,
                    ( select  'CPU'
                              ,round(value/100,3) cpu 
                      from    v$sysmetric 
                      where   metric_name = 'CPU Usage Per Sec' 
                      and     group_id=2
                    ) aas
            """)
            for wait in cursor:
                wait_name = wait[0]
                wait_value = wait[1]
                result_string = "oracle_wait_class,fqdn={0},delphix={1},db={2},wait_class={3} wait_value={4} {timestamp}".format(fqdn, self.delengine, self.sid, re.sub(' ', '_', wait_name), wait_value, timestamp=self.timestamp) 
                print(result_string)
                data.append(result_string)


    def sysmetrics(self, data):
        with self.connection.cursor() as cursor:
            cursor.execute("""
            select METRIC_NAME,
                   VALUE,
                   METRIC_UNIT 
            from   v$sysmetric 
            where  group_id = 2
            """)
            for metric in cursor:
                metric_name = metric[0]
                metric_value = metric[1]
                result_string = "oracle_sysmetric,fqdn={0},delphix={1},db={2},metric_name={3} metric_value={4} {timestamp}".format(fqdn,self.delengine, self.sid,re.sub(' ', '_', metric_name),metric_value, timestamp=self.timestamp)
                print(result_string)
                data.append(result_string)

    def fraused(self, data):
        with self.connection.cursor() as cursor:
            cursor.execute("""
            select round((SPACE_USED-SPACE_RECLAIMABLE)*100/SPACE_LIMIT,1) 
            from   V$RECOVERY_FILE_DEST
            """)
            for frau in cursor:
                fra_used = frau[0]
                result_string = "oracle_fra_pctused,fqdn={0},delphix={1},db={2} fra_pctused={3} {timestamp}".format(fqdn,self.delengine, self.sid,fra_used, timestamp=self.timestamp)
                print(result_string)
                data.append(result_string)

    def fsused(self, data):
     fss = ['/oracle', '/data']
     for fs in fss:
            df = subprocess.Popen(["df","-P",fs], stdout=subprocess.PIPE)
            output = df.communicate()[0]
            total=re.sub('%','',output.split("\n")[1].split()[1])
            used=re.sub('%','',output.split("\n")[1].split()[2])
            pctused=re.sub('%','',output.split("\n")[1].split()[4])
            result_string = "oracle_fs_pctused,fqdn={0},fs_name={1} oraclefs_pctused={2},oraclefs_alloc={3},oraclefs_used={4} {timestamp}".format(fqdn,fs,pctused,total,used, timestamp=self.timestamp)
            print(result_string)
            data.append(result_string)

    def waitstats(self, data):
        with self.connection.cursor() as cursor:
            cursor.execute("""
            select /*+ ordered use_hash(n) */
                    n.wait_class wait_class
                    ,n.name wait_name
                    ,m.wait_count  cnt
                    ,nvl(round(10*m.time_waited/nullif(m.wait_count,0),3) ,0) avg_ms
            from    v$eventmetric m,
                    v$event_name n
            where   m.event_id=n.event_id
            and     n.wait_class <> 'Idle' 
            and     m.wait_count > 0 order by 1
            """)
            for wait in cursor:
                wait_class = wait[0]
                wait_name = wait[1]
                wait_cnt = wait[2]
                wait_avgms = wait[3]
                result_string = "oracle_wait_event,fqdn={0},delphix={1},db={2},wait_class={3},wait_event={4} count={5},latency={6} {timestamp}".format(fqdn, self.delengine, self.sid,re.sub(' ', '_', wait_class), re.sub(' ','_',wait_name),wait_cnt,wait_avgms, timestamp=self.timestamp)
                print(result_string)
                data.append(result_string)

    def resource(self, data):
        with self.connection.cursor() as cursor:
            cursor.execute("""
            SELECT resource_name
                   ,current_utilization
                   ,CASE WHEN TRIM(limit_value) LIKE 'UNLIMITED' THEN '-1' 
                         ELSE TRIM(limit_value) 
                    END as limit_value
                   ,CASE WHEN TRIM(limit_value) LIKE 'UNLIMITED' THEN 0
                         WHEN to_number(TRIM(limit_value)) = 0 THEN 0
                         ELSE round(current_utilization * 100 / to_number(TRIM(limit_value)),2) 
                    END as usage_percent
            FROM   v$resource_limit
            """)
            for rsc in cursor:
                resource_name = rsc[0]
                current_utilization = rsc[1]
                limit_value = rsc[2]
                usage_percent = rsc[3]
                result_string = "oracle_resource,fqdn={fqdn},delphix={delphix},db={db},resource_name={resource_name} current_utilization={current_utilization},limit_value={limit_value},percent_used={usage_percent} {timestamp}".format(fqdn = fqdn, delphix = self.delengine, db = self.sid, resource_name = re.sub(' ', '_', resource_name), current_utilization = current_utilization, limit_value = limit_value, usage_percent = usage_percent, timestamp=self.timestamp)
                print(result_string)
                data.append(result_string)

    def sysstat(self, data):
        with self.connection.cursor() as cursor:
            cursor.execute("""
            SELECT name, 
                   value 
            FROM   v$sysstat 
            WHERE name IN ('parse count (total)', 'execute count', 'user commits', 'user rollbacks')
            """)
            for row in cursor:
                stat_name = row[0]
                value = row[1]
                result_string = "oracle_sysstat,fqdn={fqdn},delphix={delphix},db={db},stat_name={stat_name} value={value} {timestamp}".format(fqdn = fqdn, delphix = self.delengine, db = self.sid, stat_name = re.sub(' ', '_', stat_name), value = value, timestamp=self.timestamp)
                print(result_string)
                data.append(result_string)

    def process(self, data):
        with self.connection.cursor() as cursor:
            cursor.execute("""
            SELECT COUNT(*) as count 
            FROM   v$process
            """)
            for row in cursor:
                value = row[0]
                result_string = "oracle_process,fqdn={fqdn},delphix={delphix},db={db} value={value} {timestamp}".format(fqdn = fqdn, delphix = self.delengine, db = self.sid, value = value, timestamp=self.timestamp)
                print(result_string)
                data.append(result_string)

    def sessionstat(self, data):
        with self.connection.cursor() as cursor:
            cursor.execute("""
            SELECT status,
                   type,
                   COUNT(*) as value
            FROM   v$session
            GROUP BY status, type
            """)
            for row in cursor:
                status = row[0]
                type = row[1]
                value = row[2]
                result_string = "oracle_sessionstat,fqdn={fqdn},delphix={delphix},db={db},status={status},type={type} value={value} {timestamp}".format(fqdn = fqdn, delphix = self.delengine, db = self.sid, status = re.sub(' ', '_', status), type = type,  value = value, timestamp=self.timestamp)
                print(result_string)
                data.append(result_string)
                
    ## Daily 로 수행하는 Batch 정의
    
    # 테이블 스페이스
    def tbsstats(self, data):
        with self.connection.cursor() as cursor:
            cursor.execute("""
            select  df.tablespace_name "Tablespace",
                    round(df.TBS_byte / 1048576, 2) "Total(MB)",
                    round((df.TBS_byte - fs.Free_byte)/ 1048576, 2) "Used(MB)",
                    round(fs.Free_byte / 1048576, 2) "Free(MB)",
                    round((fs.Free_byte / df.TBS_byte) * 100, 0) "Free(%)",
                    round(fs.Max_free / 1048576, 2) "MaxFree(MB)"
            from    (
                        select    tablespace_name,
                                    sum(bytes) TBS_byte
                        from      dba_data_files
                        group by  tablespace_name
                    ) df,
                    (
                        select    tablespace_name,
                                    max(bytes) Max_free,
                                    sum(bytes) Free_byte,
                                    count(*) pieces
                        from      dba_free_space
                        group by  tablespace_name
                    ) fs,
                    (
                        select    tablespace_name,
                                    initial_extent,
                                    next_extent
                        from      dba_tablespaces
                    ) db
            where   df.tablespace_name = db.tablespace_name
            and     df.tablespace_name = fs.tablespace_name(+)
            """)
            for row in cursor:
                tbs_name = row[0]
                total_size_mb = row[1]
                used_space_mb = row[2]
                free_space_mb = row[3]
                free_percent = row[4]
                max_free_mb = row[5]
                result_string = "oracle_tablespaces,fqdn={0},delphix={1},db={2},tbs_name={3} total_size_mb={4},used_space_mb={4},free_space_mb={5},free_percent={6},max_free_mb={7} {timestamp}".format(fqdn, self.delengine, self.sid, re.sub(' ', '_', tbs_name), total_size_mb,used_space_mb,free_space_mb,free_percent,max_free_mb, timestamp=self.timestamp)
                print(result_string)
                data.append(result_string)
                
    # ASM 용량 쿼리 -- 하루 한번 수행  daily 오전 8시에 수행         
    def diskgroupstat(self, data):
        with self.connection.cursor() as cursor:
            cursor.execute("""
            select  group_number "Group#",
                    name,
                    total_mb/1024 TOTAL_GB,
                    round((total_mb - USABLE_FILE_MB)/1024,2) USED_GB,
                    USABLE_FILE_MB/1024 USABLE_FILE_GB,
                    free_mb/1024 FREE_GB,
                    100-round(free_mb/total_mb*100) "usgae(%)",
                    ((FREE_MB - REQUIRED_MIRROR_FREE_MB))/1024 USABLE_CALC_GB,
                    type,
                    state
            from    v$asm_diskgroup;
            """)
            for row in cursor:
                dg_number = row[0]
                dg_name = row[1]
                total_gb = row[2]
                used_gb = row[3]
                usable_file_gb = row[4]
                free_gb = row[5]
                percent_used = row[6]
                usable_calc_gb = row[7]
                result_string = "oracle_diskgroupstat,fqdn={fqdn},delphix={delphix},db={db},diskgroup={dg_name} diskgroup_number={dg_number} total_gb={total},used_gb={used_gb},usable_file_gb={usable_file_gb},ree_gb={free_gb},percent_used={percent_used},usable_calc_gb={usable_calc_gb} {timestamp}".format(fqdn = fqdn, delphix = self.delengine, db = self.sid, dg_name = re.sub(' ', '_', dg_name), dg_number = re.sub(' ', '_', dg_number), total_gb = total_gb, used_gb = used_gb, usable_file_gb = usable_file_gb, free_gb = free_gb, percent_used = percent_used, usable_calc_gb = usable_calc_gb, timestamp=self.timestamp)
                print(result_string)
                data.append(result_string)
                
    # ASM 용량 쿼리 -- 하루 한번 수행  daily 오전 8시에 수행         
    def tempdef(self, data):
        with self.connection.cursor() as cursor:
            cursor.execute("""
            select  group_number "Group#",
                    name,
                    total_mb/1024 TOTAL_GB,
                    round((total_mb - USABLE_FILE_MB)/1024,2) USED_GB,
                    USABLE_FILE_MB/1024 USABLE_FILE_GB,
                    free_mb/1024 FREE_GB,
                    100-round(free_mb/total_mb*100) "usgae(%)",
                    ((FREE_MB - REQUIRED_MIRROR_FREE_MB))/1024 USABLE_CALC_GB,
                    type,
                    state
            from    v$asm_diskgroup;
            """)
            for row in cursor:
                dg_number = row[0]
                dg_name = row[1]
                total_gb = row[2]
                used_gb = row[3]
                usable_file_gb = row[4]
                free_gb = row[5]
                percent_used = row[6]
                usable_calc_gb = row[7]
                result_string = "oracle_diskgroupstat,fqdn={fqdn},delphix={delphix},db={db},diskgroup={dg_name} diskgroup_number={dg_number} total_gb={total},used_gb={used_gb},usable_file_gb={usable_file_gb},ree_gb={free_gb},percent_used={percent_used},usable_calc_gb={usable_calc_gb} {timestamp}".format(fqdn = fqdn, delphix = self.delengine, db = self.sid, dg_name = re.sub(' ', '_', dg_name), dg_number = re.sub(' ', '_', dg_number), total_gb = total_gb, used_gb = used_gb, usable_file_gb = usable_file_gb, free_gb = free_gb, percent_used = percent_used, usable_calc_gb = usable_calc_gb, timestamp=self.timestamp)
                print(result_string)
                data.append(result_string)
                

class InfluxDBWriter():
    def __init__(self, host, port, user, passwd, dbname):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.dbname = dbname

    def writeData(self, data):
        try:
            write_client = InfluxDBClient(host= self.host, port= self.port, username= self.user, password= self.passwd, database= self.dbname)
            # Write data to Target DB
            write_client.write_points(data, database='oracle', time_precision='s', batch_size=app.config['INFLUX_WRITE_BATCH_SIZE'], protocol='line')
            
        except InfluxDBClientError as err:
            row_count = 0
            log.error('Write to database failed: %s' % err)

class KafkaSender():
    def __init__(self, bootstarap_servers, topic, batch_size, linger_ms):
        self.bootstrap_servers = bootstarap_servers
        self.topic = topic
        self.batch_size = batch_size
        self.linger_ms = linger_ms

    def produceMessage(self, data):
        try:

            sendData = {'metrics': data}
            producer = KafkaProducer(bootstrap_servers = self.bootstrap_servers, batch_size = self.batch_size, linger_ms = self.linger_ms, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            ### KafkaConsumer(value_deserializer=lambda v: json.loads(v.decode('utf-8')))

            producer.send(self.topic, sendData)
            producer.flush()
            
        except KafkaError as err:
            row_count = 0
            log.error('produce message failed: %s' % err)


def oracle_exporter():

    username = app.config['DB_USER']
    passwd =  app.config['DB_PASSWORD']
    output_format = app.config['OUTPUT_FORMAT']
    timestamp = str(int(time()))

    print("timestamp : " + timestamp)

    data = []

    for sid in app.config['SID']:
        stats = OraStats(username, passwd, sid, timestamp)
        stats.waitclassstats(data)
        stats.waitstats(data)
        stats.sysmetrics(data)
        stats.fraused(data)
        stats.resource(data)
        stats.sysstat(data)
        stats.process(data)
        stats.sessionstat(data)
        #stats.fsused(data)

    # output Target  
    output_target = app.config['OUTPUT_TARGET']

    if output_target == 'influx':
        ### write data to influx
        ## Write Datapoints to target Influx DB
        influx_db_host = app.config['INFLUX_DB_SERVER']
        influx_db_port = app.config['INFLUX_DB_PORT']
        influx_db_user = app.config['INFLUX_DB_ID']
        influx_db_password = app.config['INFLUX_DB_PW']
        influx_dbname = app.config['INFLUX_DB_NAME']

        influxWriter = InfluxDBWriter(influx_db_host, influx_db_port, influx_db_user, influx_db_password, influx_dbname)
        influxWriter.writeData(data)
    
    elif output_target == 'kafka':

        kafkaBootStrapServers = app.config['KAFKA_BOOTSTRAP_SERVERS']
        kafkaTopic = app.config['KAFKA_TOPIC']
        kafkaBatchSize = int(app.config['KAFKA_BATCH_SIZE'])
        kafkaLingerMs = int(app.config['KAFKA_LINGER_MS'])

        kafkaSender = KafkaSender(kafkaBootStrapServers, kafkaTopic, kafkaBatchSize, kafkaLingerMs)
        kafkaSender.produceMessage(data)

    else:
        print(data)
        
def oracle_ontime_exporter():

    username = app.config['DB_USER']
    passwd =  app.config['DB_PASSWORD']
    output_format = app.config['OUTPUT_FORMAT']
    timestamp = str(int(time()))

    print("timestamp : " + timestamp)

    data = []

    for sid in app.config['SID']:
        stats = OraStats(username, passwd, sid, timestamp)
        stats.tbsstats(data)
        stats.diskgroupstat(data)

    # output Target  
    output_target = app.config['OUTPUT_TARGET']

    if output_target == 'influx':
        ### write data to influx
        ## Write Datapoints to target Influx DB
        influx_db_host = app.config['INFLUX_DB_SERVER']
        influx_db_port = app.config['INFLUX_DB_PORT']
        influx_db_user = app.config['INFLUX_DB_ID']
        influx_db_password = app.config['INFLUX_DB_PW']
        influx_dbname = app.config['INFLUX_DB_NAME']

        influxWriter = InfluxDBWriter(influx_db_host, influx_db_port, influx_db_user, influx_db_password, influx_dbname)
        influxWriter.writeData(data)
    
    elif output_target == 'kafka':

        kafkaBootStrapServers = app.config['KAFKA_BOOTSTRAP_SERVERS']
        kafkaTopic = app.config['KAFKA_TOPIC']
        kafkaBatchSize = int(app.config['KAFKA_BATCH_SIZE'])
        kafkaLingerMs = int(app.config['KAFKA_LINGER_MS'])

        kafkaSender = KafkaSender(kafkaBootStrapServers, kafkaTopic, kafkaBatchSize, kafkaLingerMs)
        kafkaSender.produceMessage(data)

    else:
        print(data)


sched = BackgroundScheduler(daemon=True)
# debugging
# sched.add_job(get_app_id,'cron',hour='13',minute='31')
# sched.add_job(get_app_id,'interval', seconds=30)
# sched.add_job(write_smd_bypass_status, 'interval', seconds=30)
sched.add_job(oracle_exporter, 'interval', seconds=5)
sched.add_job(oracle_ontime_exporter,'cron',hour='08',minute='00')
sched.start()

app = Flask(__name__)

app.config['JSON_ADD_STATUS'] = False
app.config['JSON_AS_ASCII'] = False

if __name__ == '__main__':
    log = logging.getLogger()
    log.setLevel(logging.DEBUG) #DEBUG
    #log.setLevel(logging.WARNING) #for Production
    app.debug = False
    app.config.from_object('config.ProductionConfig')
    app.run(host='0.0.0.0', port=5060)