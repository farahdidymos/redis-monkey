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
