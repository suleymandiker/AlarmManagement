package com.alarm.AlarmManagement;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.TimerTask;

import com.alarm.AlarmManagement.App;

public class AlarmTimerRun extends TimerTask {
	
	@Override
	public void run() {
		

		WebSchedulerAlarms webSchedulerAlarms=new WebSchedulerAlarms();
		 
		try {
			 

			webSchedulerAlarms.getSCHDTFPlans();
			webSchedulerAlarms.getSchETLPlans();
			 
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

}
