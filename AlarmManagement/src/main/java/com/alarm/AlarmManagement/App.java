package com.alarm.AlarmManagement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import com.alarm.AlarmManagement.*;


public class App 
{
    public static void main( String[] args ) throws Exception
    {
	
  	  TimerTask  AlarmManagementTask  = new  AlarmTimerRun();
  	  Timer AlarmTimer = new Timer();

  	  
  	  AlarmTimer.scheduleAtFixedRate(AlarmManagementTask,500,900000);     
    	

    }
    
    



	
	
}
