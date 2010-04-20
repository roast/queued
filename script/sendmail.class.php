<?php

/**
 * 邮件发送脚本
 *
 * @author guojunming <guojunming@corp.the9.com>
 * 
 * $Id: sendmail.class.php 290 2010-03-21 16:44:32Z guojunming $
 */

class sendmail
{

    /**
     * 日志操作句柄
     *
     * @var Object
     */
    private $log;   
	
	
    
    public function __construct()
    {
        $this->log = getLog('sendmail');
    }
    /**
     * 执行函数
     *
     * @param String $argv
     */
    public function exec($data)
    {
		// 接收参数
		$mail_info = unserialize(base64_decode($data));  
		$subject = "=?GB2312?B?".base64_encode($mail_info['subject'])."?=";

		mail($mail_info['item'], $subject, $mail_info['content'], $mail_info['header'], $mail_info['config']);
		$mail_info['subject']= iconv('GB2312','UTF-8',$mail_info['subject']);
		$this->log->setData("\r\nTO:".$mail_info['item'].' Subject:'.$mail_info['subject'].' '.date('Y-m-d H:i:s'))->write();  
    }   
}
