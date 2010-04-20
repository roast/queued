#! /app/php/bin/php
<?php
/**
 * 队列处理分发守护进程
 * - 创建多个队列处理程序进程
 * - 创建队列健康检查程序,如为僵死进程则关闭
 * - 单个进程处理队列相应脚本
 *
 * 需要可执行权限 chmod +x queued.php
 * 
 * @author 张立冰 <zhanglibing@corp.the9.net>
 *  
 * $Id: queued.php 7157 2009-06-26 10:17:49Z libing $
 */

set_time_limit(0);
ini_set('memory_limit', '512M');
define('ROOT', '/data/server/');

//包含通用文件
require_once(ROOT . 'lib/base.inc.php');

//对信号进行处理，包括退出和重新启动
function sig_handler($signo) 
{
	$log = getLog('queue/queued');
	$log->setData("\r\n" . 'Signal Receive:' . $signo . ' ' . date('Y-m-d H:i:s'));
	
	$pids = @file(ROOT . 'logs/queue/pids');
	if (is_array($pids))
	{
		foreach ($pids as $pid)
		{
			list(, , $pid) = explode('_', $pid);
			if (!empty($pid))
			{
				$pid = intval($pid);
				@posix_kill($pid, SIGTERM);
				
				if (file_exists(ROOT . 'logs/queue/pid/' . $pid))
					@unlink(ROOT . 'logs/queue/pid/' . $pid);
			}
		}
	}
	
    switch ($signo) 
    {
        case SIGHUP:
        	@popen(ROOT . 'daemon/queue/queued.php 2>&1 > /dev/null &', "r");
            break;
    }
    
    $log->setData("\r\n" . 'Signal Process Finished:' . date('Y-m-d H:i:s'))->write();
    
    exit();
}

//如果没有参数则表示启动队列系统
if ($argc == 1)
{
	$log = getLog('queue/queued');
	$log->setData('Queued Starting:' . date('Y-m-d H:i:s'));
	
	if (@!file_exists(ROOT . 'logs/queue'))
		@mkdir(ROOT . 'logs/queue');
		
	if (@!file_exists(ROOT . 'logs/queue/pid'))
		@mkdir(ROOT . 'logs/queue/pid');
		
	//创建用于记录健康检查的PID记录文件	
	@file_put_contents(ROOT . 'logs/queue/pids', '');
	
	//开启队列进程
	if (is_array($queue_server_ip))
	{
		//默认的健康检查的时间值
		$health_timeout = 60;
		
		foreach ($queue_server_ip as $key => $queue_item)
		{
			for ($i = 0; $i < $queue_item['thread']; $i++)
			{
				@popen(ROOT . 'daemon/queue/queued.php ' . $key . ' ' . $i . '  2>&1 > /dev/null &', "r");
			}
			
			if ($queue_item['timeout'] < $health_timeout)
				$health_timeout = $queue_item['timeout'];
				
			$log->setData("\r\n" . 'Started Queue[' . $queue_item['mckey'] . ']:' . date('Y-m-d H:i:s'));
		}
		
		//启动健康检查程序
		@popen(ROOT . 'daemon/queue/queued.php health ' . $health_timeout . '  2>&1 > /dev/null &', "r");
		$log->setData("\r\n" . 'Started Health Check:' . date('Y-m-d H:i:s'));
	}
	
	$log->setData("\r\n" . 'Queued Started:' . date('Y-m-d H:i:s'));
	$log->write();
	
	exit();
}

//启动健康检查程序	-- 一份钟检查一次进程状态
if ($argv[1] == 'health')
{
	declare(ticks = 1);
	
	pcntl_signal(SIGTERM, 'sig_handler');
	pcntl_signal(SIGHUP,  'sig_handler');
	pcntl_signal(SIGUSR1, 'sig_handler');
	
	$log = getLog('queue/health');
	while (true) 
	{
		//每60秒执行一次健康检查
		sleep(60);
		
		$pids = @file(ROOT . 'logs/queue/pids');
		if (is_array($pids))
		{
			//找出所有超时的进程
			$unset_pids = array();
			foreach ($pids as $k => $spid)
			{
				$pid = trim($spid);
				
				list($key, $i, $pid) = explode('_', $pid);
				if (!empty($pid))
				{
					$config = $queue_server_ip[$key];
					
					$time = @file_get_contents(ROOT . 'logs/queue/pid/' . $pid);
					$stat = posix_getpgid($pid);
					if (empty($stat) || ((time() - $time) > $config['timeout'])) 		//已经超时则重新开启
						$unset_pids[$k] = $spid;
				}
			}
			
			if (count($unset_pids) > 0)
			{
				$log->setData("\r\n" . 'Restart Pids:' . implode(',', $unset_pids) . ' ' . date('Y-m-d H:i:s'));
				
				$fp = @fopen(ROOT . 'logs/queue/pids', 'w+');
				if ($fp)
				{
					//将不需要重启的进程写入原进程文件
					foreach ($pids as $k => $pid)
					{
						if (empty($unset_pids[$k]))
						{
							$pid = trim($pid);
							@fwrite($fp, $pid . "\n");
						}
					}
					
					@fclose($fp);
					
					//重启需要重启的新进程
					foreach ($unset_pids as $pid)
					{
						$pid = trim($pid);
						list($key, $i, $pid) = explode('_', $pid);
						$stat = posix_getpgid($pid);
						if (empty($stat) || posix_kill($pid, SIGTERM))
						{
							@popen(ROOT . 'daemon/queue/queued.php ' . $key . ' ' . $i . '  2>&1 > /dev/null &', "r");
							
							if (file_exists(ROOT . 'logs/queue/pid/' . $pid))
								@unlink(ROOT . 'logs/queue/pid/' . $pid);
						}
					}
				}
				
				$log->setData('Restarted ' . date('Y-m-d H:i:s') . "\r\n");
				$log->write();
			}
		}
	}
}

//如果参数带有两个参数则表示启动子队列程序
if ($argc == 3)
{
	$log = getLog('queue/queued');
	
	if (DEBUG)
		$debug = getLog('queue/debug');
	
	$fp = @fopen(ROOT . 'logs/queue/pids', 'a');
	if (!$fp)
	{
		$log->setData("\r\n" . 'Init Queue Thread Failed.' . $argv[1] . '_' . $argv[2]);
		$log->write();
		
		exit();
	}
	
	//记录PID以做健康检查使用
	@fwrite($fp, $argv[1] . '_' . $argv[2] . '_' . posix_getpid() . "\n");
	fclose($fp);
	
	$config = $queue_server_ip[intval($argv[1])];
	$server = $config['server'];
	
	if (DEBUG)
		$debug->setData(array('config:', print_r($config, true)))->write();
				
	//脚本对应的对象沲
	$script_objs = array();
	
	//将MC操作做为全局变量
	$mc = new Memcache();
	foreach($server as $m)
	{
		$mc->addServer($m['host'], $m['port']);
	}

	//将队列开始时间写入PID文件
	@file_put_contents(ROOT . 'logs/queue/pid/' . posix_getpid(), time());
		
	//读取数据开始
	while (1) 
	{
		//记录队列开始时间
		$start = time();
		
		//读取队列消息进行处理
		while($q = $mc->get($config['mckey']))
		{
			//每超过timeout时间就写一次PID时间文件用于健康检查
			if ((time() - $start) > $config['timeout'])
			{
				$start = time();
				@file_put_contents(ROOT . 'logs/queue/pid/' . posix_getpid(), time());
			}
			
			if (empty($q))
				break;
				
			list($class, $args) = explode(' ', $q);
			if (empty($script_objs[$class]))
			{
				$file = ROOT . 'daemon/queue/script/' . $class . '.class.php';
				if (file_exists($file))
				{
					require_once($file);
					$script_objs[$class] = new $class();
				}
				else 
				{
					$log->setData("\r\n" . 'Bad Format:' . $q)->write();
					continue;
				}
			}
			
			//处理请求
			$o = $script_objs[$class];
			$r = $o->exec($args);
			
			if (DEBUG)
				$debug->setData(array($q, $r))->write();
				
			//检查内存是否超过限制
			if (memory_get_usage() > $config['memory'])
			{
				$log->setData("\r\n" . 'Memory Out[' . posix_getpid() . ']:' . memory_get_usage() . ' ' . date('Y-m-d H:i:s'))->write();
				
				$pids = @file(ROOT . 'logs/queue/pids');
				if (is_array($pids))
				{
					$fp = @fopen(ROOT . 'logs/queue/pids', 'w+');
					if ($fp)
					{
						//将不需要重启的进程写入原进程文件
						foreach ($pids as $k => $spid)
						{
							$spid = trim($spid);
							list($key, $i, $pid) = explode('_', $spid);
							if ($pid != posix_getpid())
								@fwrite($fp, $spid . "\n");
						}
					}
						
					@fclose($fp);
					
					@popen(ROOT . 'daemon/queue/queued.php ' . $argv[1] . ' ' . $argv[2] . '  2>&1 > /dev/null &', "r");
					
					if (file_exists(ROOT . 'logs/queue/pid/' . posix_getpid()))
						@unlink(ROOT . 'logs/queue/pid/' . posix_getpid());
					
					exit();
				}				
			}	
		}
		
		//如果队列当前为空则暂停2秒
		sleep(2);
	}
	
	$mc->close();
}
?>