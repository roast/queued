<?php

/**
 * squid缓存刷新脚本，用于刷新用户上传头像和相册
 *
 * @author guojunming <guojunming@corp.the9.com>
 * 
 * $Id: squid.class.php 6924 2009-06-04 12:03:09Z libing $
 */

class squid 
{
        
    private $squid;
    private $fsocket;
    private $log;

    function __construct()
    {
        global $cfg;
        // squid 服务器列表
        $this->squid = $cfg['squid'];
                
        //日志操作句柄
        $this->log = getLog('squid');
    }
        
        /**
         * 建立socket连接
         * 
         */
        private function connect()
        {
            foreach ($this->squid as $host) 
                {
                        $tmp['fp'] = @fsockopen($host['host'], intval($host['port']), $errono, $errstr,intval($host['timeout']));
                        $tmp['host']= $host['host'].':'.$host['port'];
                        if ($errstr)
                            $this->log->setData("\r\n".$host['host'].':'.$host['port'].' '.$errstr. ' '. date('Y-m-d H:i:s'))->write();
                        else 
                            $this->fsocket[] = $tmp;
                }
        }
        
        /**
         * 关闭连接
         * 
         */
        private function disconnect()
        {
                foreach ($this->fsocket as $fsocket)
                    @fclose($fsocket['fp']);
        }
        
        /**
         * 建立发送头
         * 
         */
        private function buildHead($url)
        {
                $url_component = parse_url($url);
                $head = "PURGE {$url_component['path']} HTTP/1.0\r\n";
                $head .= "Accept: */*\r\n";
                $head .= "Host: {$url_component['host']}\r\n";
                $head .= "Cache-Control: no-cache\r\n";
                $head .= "\r\n";
                return $head;
        }
        
        /**
         * 执行函数
         *
         * @param String $argv
         */     
        public function exec($url)
        {
                $this->connect();
                $head = $this->buildHead($url);
                foreach($this->fsocket as $fsocket)
                {
                    if(FALSE != $fsocket['fp']) 
                    {
                            if(@fwrite($fsocket['fp'] , $head)) 
                            {
                                    if (! $line = @fread($fsocket['fp'] , 13))
                                        $this->log->setData("\r\n".$fsocket['host'].' '.$url.' fsocket_read_error '. date('Y-m-d H:i:s'))->write();
                                    else 
                                    {
                                        if(!preg_match("/HTTP\/1\.0 404/",$line) && !preg_match("/HTTP\/1\.0 200/",$line))
                                            $this->log->setData("\r\n".$fsocket['host'].' '.$url.' '.$line.' '. date('Y-m-d H:i:s'))->write();
                                    }
                            }
                            #else
                            #    $this->log->setData("\r\n".$fsocket['host'].' '.$url.' fsockopen_write_error '. date('Y-m-d H:i:s'))->write();
                    }
                    else
                            $this->log->setData("\r\n".$fsocket['host'].' '.$url.' fsocket_open_error '. date('Y-m-d H:i:s'))->write();
                }
                $this->disconnect();
        }
}
