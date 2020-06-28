# -*- coding:utf-8 -*-
"""

modules for redis info parse

"""

import json
import os
import time

import paramiko
import redis
import logging


class RedisInfo(object):
    """
    redis info

    """

    RETRY_TIMES = 3

    INFO_KEY = [

        # ---- server :
        'redis_version',
        'uptime_in_seconds',
        'uptime_in_days',


        # ---- memory
        'used_memory',
        'used_memory_rss',
        'mem_fragmentation_ratio',
        'total_system_memory',

        # ---- Stats :
        'total_connections_received',
        'total_commands_processed',
        'instantaneous_ops_per_sec',

        'total_net_input_bytes',
        'total_net_output_bytes',
        'instantaneous_input_kbps',
        'instantaneous_output_kbps',

        'rejected_connections',

        'expired_keys',
        'evicted_keys',
        'keyspace_hits',
        'keyspace_misses',

        # ---- clients :
        'connected_clients',
        'blocked_clients',

        # ---- Keyspace

        # total_keys

        # ---- commandstats

    ]

    def __init__(self, host="localhost", port=6379, password=""):

        self.host = host
        self.port = port
        self.password = password

        self.monitor_info = None  # dict: storage monitor information

        self._redis_client = None
        # self._auth_init()

        self.redis_connection_init()

    def redis_connection_init(self):
        # connection init
        try:
            pool = redis.ConnectionPool(host=self.host, port=self.port, db=0,
                                        password=self.password)
            self._redis_client = redis.Redis(connection_pool=pool)
            RETRY_TIMES = 3
        except Exception as ex:
            logging.error(ex)
        finally:
            if RETRY_TIMES > 0:
                RETRY_TIMES = RETRY_TIMES - 1
                self.redis_connection_init()

    def _auth_init_from_env(self):
        # redis auth info
        self.host = str(os.environ['REDIS_ADDRESS'])
        self.port = int(os.environ['REDIS_PORT'])
        self.password = str(os.environ['REDIS_PASSWORD'])

    def rdb_info(self, section="Stats"):
        # get stats from redis server
        info = self._redis_client.info(section)    # redis info
        return info

    @staticmethod
    def state(f):
        format_unit = f"{f:.2f}"
        return format_unit

    @staticmethod
    def get_cpu():
        # system cpu info
        num = 0
        with open('/proc/cpuinfo') as fd:
            for line in fd:
                if line.startswith('processor'):
                    num += 1
                if line.startswith('model name'):
                    cpu_model = line.split(':')[1].strip().split()
                    cpu_model = cpu_model[0] + ' ' + cpu_model[2] + ' ' + cpu_model[-1]
        cpu_info = f"CPU : cpu_num : {num}   cpu_model : {cpu_model}"
        return cpu_info

    @staticmethod
    def get_memory():
        # system memory info
        #   kb : return GB
        with open('/proc/meminfo') as fd:
            for line in fd:
                if line.startswith('MemTotal'):
                    mem = int(line.split()[1].strip())
                    break

        mem_info = f'MemTotal: {mem / 1024 / 1024 :.2f} GB'
        return mem_info
    def pustil_disk_io(self):
        """
         固定两个盘: 1) 系统盘vda1（挂载点 '/' ) , 数据盘vdd (挂载点'/data'))
         ***** 默认vdd  对应 lv-redis 逻辑卷 *****
        :return:
        """

        disks_info = {}
        disk_partitions = psutil.disk_partitions()
        disk_io = psutil.disk_io_counters(perdisk=True)

        for d in disk_partitions:
            # print('%s %10s %6s %10s' % (d.device, d.mountpoint,  d.fstype, d.opts))
            di = psutil.disk_usage(d.mountpoint)
            disk_name = d.device.rsplit('/', 1)[1]

            # redis部署环境中: 将/dev/mapper/vg--redis-lv--redis 对应为 vdd
            if disk_name.startswith('vg--redis'):
                disk_name = 'vdd'

            dio = disk_io[disk_name]
            disks_info[disk_name] = {
                'disk_utilization': di.percent,
                'disk_io': (bt_to_kb(dio.read_bytes),
                            bt_to_kb(dio.write_bytes))
            }

        # 获取磁盘io平均等待时间 ioawait (单位 ms)
        disks_ioawait = self.proc_diskstats(list(disks_info.keys()))
        for disk_name, val in disks_ioawait.items():
            # ioawait = delta(ruse+wuse)/delta(rio+wio)  单位: ms
            disks_info[disk_name]['disk_ioawait'] = val    # (rio, ruse, wio, wuse)

        # process monitor info
        self.process_disk_info(disks_info)

    def redis_benchmark():
    # redis benchmark test commands
    # info store

    with codecs.open('redis_benchmark_test.csv', 
                     'w', encoding = 'utf-8-sig', errors = 'ignore')as f:

        csv_writer = csv.writer(f)

        pinfo = platform_info()

        cpu_info = get_cpu()
        mem_info = get_memory()
        csv_writer.writerow((pinfo, ))
        csv_writer.writerow([cpu_info, ])
        csv_writer.writerow([mem_info, ])
        csv_writer.writerow(headers_line)

        global scmds_benchmark_str

        for index, (c, n, r, P)in enumerate(bh_vars_list):

            cmd = scmds_benchmark_str.format(
                scmd = scmd, c = c, n = n, r = r, P = P)

            re_lst = [c, n, r, P]
            re_lst.extend(redis_benchmark_cmd(cmd))
            if re_lst:
                re_lst.insert(0, f"{index})")
                csv_writer.writerow(re_lst)


    def proc_net_dev(self):
        """
        内(外) 网络 带宽及包量信息获取
            redis部署环境中: 网卡名固定
            'eth0' 外网
            'eth1' 内网
        信息获取文件 /proc/net/dev
            最左边的表示接口的名字，Receive表示收包，Transmit表示发包
            bytes表示收发的字节数； packets表示收发正确的包量；errs表示收发错误的包量；drop表示收发丢弃的包量；
        :return:
        """

        with open('/proc/net/dev') as fd:

            for line in fd.readlines()[2:]:
                line = line.strip()
                if line:
                    line = re.split(r'[\s]+', line, flags=re.M)
                    # print(line)
                    assert len(line) > 16    # 有些系统记录不同
                    net_dev = line[0].rstrip(':')

                    if net_dev == 'lo':    # ignore 'lo'
                        continue
                    net_info = (bt_to_mb(line[1]),
                                sum(map(int, line[2:9])),
                                bt_to_mb(line[9]),
                                sum(map(int, line[10:17])))

                    # process monitor info
                    self.process_net_info(net_dev, net_info)


    def process_disk_info(self, new_disks_info):
        """
        calculate && update the monitor information of disks
        :param new_disks_info:
        :return:
        """
        time_delta = self.time_delta    # s <秒>

        for disk_name, new_info in new_disks_info.items():

            if disk_name in self.disk_info:
                old_info = self.disk_info[disk_name]

                disk_utilization = float_format(new_info['disk_utilization'])    # %
                # read_kb_ps,       write_kb_ps,
                read_kb_ps = float_format((new_info['disk_io'][0] -
                                           old_info['disk_io'][0])/time_delta)
                write_kb_ps = float_format((new_info['disk_io'][1] -
                                            old_info['disk_io'][1])/time_delta)

                # ioawait
                # delta(ruse+wuse)/delta(rio+wio)  单位: ms
                nd_ioawait = new_info['disk_ioawait']    # set (rio, ruse, wio, wuse)
                od_dioawait = old_info['disk_ioawait']    # set
                delta_rwio = float_format(((nd_ioawait[0] - od_dioawait[0]) +
                                           (nd_ioawait[2] - od_dioawait[2])))   # kb/s
                delta_rwuse = float_format(((nd_ioawait[1] - od_dioawait[1]) +
                                            (nd_ioawait[3] - od_dioawait[3])))   # kb/s
                ioawait = float_format(delta_rwuse / delta_rwio if delta_rwio else 0)   # 增量为0则设置为0

                # store
                self.disk_monitor_info[disk_name] = (disk_utilization,
                                                     read_kb_ps, write_kb_ps,
                                                     ioawait)
            # store/update data
            self.disk_info[disk_name] = new_info

    def get_qps_tps(self):
        # tps  total_commands_processed : <Sampling interval>
        # https://github.com/me115/cppset/blob/master/redisTPS/main.cpp

        sec_start = time.time()
        cmd_num_start = self.rdb_info()['total_commands_processed']
        time.sleep(1)
        sec_end = time.time()
        info = self.rdb_info()
        cmd_num_end = info['total_commands_processed']
        qps = info['instantaneous_ops_per_sec']

        tps = (cmd_num_end - cmd_num_start) / (sec_end-sec_start)

        return qps, tps

    def alive_check(self):
        # 检测 redis进程是否存活, pid文件是否存在或者 process是否存活

        if os.path.exists(self.pid_file_path):
            pid = int(open(self.pid_file_path).readline().strip())

            if psutil.pid_exists(pid):
                rss = self.process_mem()
                # 获取redis进程的内存使用大小
                self.mem_redis_server = float_format(bt_to_mb(rss)/1024)
                self.monitor_graph_used_memory = rss
                self.redis_alive = True
                return True
            else:
                self.redis_alive = False
                return False
        else:
            self.redis_alive = False
            return False

    def process_monitor_graph_data(self):

        try:
            conn = self.redis_local_connect()
            keyspace =  conn.info("keyspace")
            if "db0" in keyspace:
                self.monitor_graph_total_keys = keyspace["db0"]["keys"]
            self.monitor_graph_connected_clients = conn.info("clients")["connected_clients"]
            self.monitor_graph_instantaneous_net_input = conn.info("stats")["instantaneous_input_kbps"]
            self.monitor_graph_instantaneous_net_output = conn.info("stats")["instantaneous_output_kbps"]
            keyspace_hits = conn.info("stats")["keyspace_hits"]
            keyspace_misses = conn.info("stats")["keyspace_misses"]
            self.monitor_graph_keyspace_hits = keyspace_hits
            latency = subprocess.getoutput("redis-cli -p {{ redisServerPort }} --latency --raw")
            self.monitor_graph_max_latency = int(str.split(latency)[1])
        except BaseException as e:
            traceback.print_exc()
            

    def log_show(self):
        #  log process
        pass

    def poll(self):
        """
        performance index
        :return: all info of INFO_KEY
        """
        db_info = {}
        for keyname, value in self.rdb_info().items():

            if keyname in self.INFO_KEY:
                db_info[keyname] = value

        self.output_info(db_info)

    def output_info(self, monitor_info):
        # output data
        pass

    def json_info(self, section="all"):
        """
        get section info from redis
        :param section:
        :return: json data
        """
        data = self.rdb_info(section)
        json_data = json.dumps(data)
        return json_data


def main():
    info = RedisInfo()
    json_info = info.json_info()
    print(json_info)


if __name__ == '__main__':
    main()
