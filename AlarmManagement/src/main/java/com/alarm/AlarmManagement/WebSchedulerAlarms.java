package com.alarm.AlarmManagement;

public class WebSchedulerAlarms {
	
	public static void getSCHDTFPlans() throws Exception {

		GlobalVariables.v_conn_SCHDTF   =  GlobalVariables.getSCHDTFConnection();
		GlobalVariables.v_conn_TERADATA =  GlobalVariables.getTERADATAConnection();

		/****************************************************************
		 * 
		 * Son kurulan massive planının ett_date ini alır
		 * 
		 *****************************************************************/

		GlobalVariables.v_stmt_SCHDTF = GlobalVariables.v_conn_SCHDTF
				.createStatement();

		GlobalVariables.v_sql = "select to_char(max(ETT_DATE),'dd.mm.yyyy') from sch2.sch_plan_jobs_v3 where plan_name='MASSIVE'";
		GlobalVariables.v_rs = GlobalVariables.v_stmt_SCHDTF
				.executeQuery(GlobalVariables.v_sql);
		while (GlobalVariables.v_rs.next()) {
			GlobalVariables.v_massive_ett_date = GlobalVariables.v_rs
					.getString(1);
		}
		GlobalVariables.v_stmt_SCHDTF.close();

		System.out.println("Massive Etl Date: "
				+ GlobalVariables.v_massive_ett_date);

		/****************************************************************
		 * 
		 * Son kurulan cognos planının ett_date ini alır
		 * 
		 *****************************************************************/

		GlobalVariables.v_stmt_SCHDTF = GlobalVariables.v_conn_SCHDTF
				.createStatement();

		GlobalVariables.v_sql = "select to_char(max(ETT_DATE),'dd.mm.yyyy') from sch2.sch_plan_jobs_v3 where plan_name='COGNOS'";
		GlobalVariables.v_rs = GlobalVariables.v_stmt_SCHDTF
				.executeQuery(GlobalVariables.v_sql);
		while (GlobalVariables.v_rs.next()) {
			GlobalVariables.v_cognos_ett_date = GlobalVariables.v_rs
					.getString(1);
		}
		GlobalVariables.v_stmt_SCHDTF.close();

		System.out.println("Cognos Etl Date: "
				+ GlobalVariables.v_cognos_ett_date);

		/****************************************************************
		 * 
		 * Son kurulan Massive Recursive planının ett_date ini alır
		 * 
		 *****************************************************************/

		GlobalVariables.v_stmt_SCHDTF = GlobalVariables.v_conn_SCHDTF
				.createStatement();

		GlobalVariables.v_sql = "select to_char(max(ETT_DATE),'dd.mm.yyyy') from sch2.sch_plan_jobs_v3 where plan_name='MASSIVE_RECURSIVE'";
		GlobalVariables.v_rs = GlobalVariables.v_stmt_SCHDTF
				.executeQuery(GlobalVariables.v_sql);
		while (GlobalVariables.v_rs.next()) {
			GlobalVariables.v_massive_recursive_ett_date = GlobalVariables.v_rs
					.getString(1);
		}
		GlobalVariables.v_stmt_SCHDTF.close();

		System.out.println("Massive Recursive Etl Date: "
				+ GlobalVariables.v_massive_recursive_ett_date);

		/****************************************************************
		 * 
		 * Son kurulan Cognos Recursive planının ett_date ini alır
		 * 
		 *****************************************************************/

		GlobalVariables.v_stmt_SCHDTF = GlobalVariables.v_conn_SCHDTF
				.createStatement();

		GlobalVariables.v_sql = "select to_char(max(ETT_DATE),'dd.mm.yyyy') from sch2.sch_plan_jobs_v3 where plan_name='COGNOS_RECURSIVE'";
		GlobalVariables.v_rs = GlobalVariables.v_stmt_SCHDTF
				.executeQuery(GlobalVariables.v_sql);
		while (GlobalVariables.v_rs.next()) {
			GlobalVariables.v_cognos_recursive_ett_date = GlobalVariables.v_rs
					.getString(1);
		}
		GlobalVariables.v_stmt_SCHDTF.close();

		System.out.println("Cognos Recursive Etl Date: "
				+ GlobalVariables.v_cognos_recursive_ett_date);

		/****************************************************************
		 * 
		 * bigdata.alarm_params tablosunda tanımlı olan jobların listesini alır.
		 * 
		 *****************************************************************/

		GlobalVariables.v_stmt_TERADATA = GlobalVariables.v_conn_TERADATA
				.createStatement();
		GlobalVariables.v_sql = "select * from bigdata.alarm_params where plan_name IN ('MASSIVE','COGNOS','MASSIVE_RECURSIVE','COGNOS_RECURSIVE') and job_status='CLEAR' AND mail_status=0";
		GlobalVariables.v_rs = GlobalVariables.v_stmt_TERADATA
				.executeQuery(GlobalVariables.v_sql);

		while (GlobalVariables.v_rs.next()) {

			GlobalVariables.v_alarm_id_list = GlobalVariables.v_alarm_id
					.append(GlobalVariables.v_rs.getString(1)).append(",")
					.toString().split(",");
			GlobalVariables.v_plan_name_list = GlobalVariables.v_plan_name
					.append(GlobalVariables.v_rs.getString(2)).append(",")
					.toString().split(",");
			GlobalVariables.v_job_name_list = GlobalVariables.v_job_name
					.append(GlobalVariables.v_rs.getString(3)).append(",")
					.toString().split(",");
			GlobalVariables.v_job_status_list = GlobalVariables.v_job_status
					.append(GlobalVariables.v_rs.getString(4)).append(",")
					.toString().split(",");
			GlobalVariables.v_job_error_detail_list = GlobalVariables.v_job_error_detail
					.append(GlobalVariables.v_rs.getString(5)).append(",")
					.toString().split(",");
			GlobalVariables.v_alarm_time_list = GlobalVariables.v_alarm_time
					.append(GlobalVariables.v_rs.getString(6)).append(",")
					.toString().split(",");

		}

		GlobalVariables.v_stmt_TERADATA.close();

		GlobalVariables.v_stmt_SCHDTF = GlobalVariables.v_conn_SCHDTF
				.createStatement();

		/****************************************************************
		 * 
		 * bigdata.alarm_params tablosunda tanımlı olan jobları SCHETL de
		 * filtreleyerek jobların statülerini kontrol eder. Error statüsünde
		 * olan joblar için mail ile alarm üretir.
		 * 
		 *****************************************************************/

		for (int i = 0; i < GlobalVariables.v_plan_name_list.length; i++) {



			if (GlobalVariables.v_plan_name_list[i].contains("MASSIVE")) {
				// System.out.println("Bu joblar massive plani jobidir.");

				GlobalVariables.v_sql = "select * from sch2.sch_plan_jobs_v3 where job_name='"
						+ GlobalVariables.v_job_name_list[i]
						+ "' and ett_date=to_date('"
						+ GlobalVariables.v_massive_ett_date
						+ "','dd.mm.yyyy') and plan_name='"
						+ GlobalVariables.v_plan_name_list[i] + "'";

				// System.out.println(GlobalVariables.v_sql);

				GlobalVariables.v_rs = GlobalVariables.v_stmt_SCHDTF
						.executeQuery(GlobalVariables.v_sql);

				while (GlobalVariables.v_rs.next()) {

					if (GlobalVariables.v_rs.getString(15).contains("ERROR")) {

						GlobalVariables.v_open_result.append("OPEN|"
								+ GlobalVariables.v_alarm_id_list[i]
								+ "|MAJOR|"
								+ GlobalVariables.v_rs.getString(13) + "|"
								+ GlobalVariables.v_plan_name_list[i] + " planındaki"
								+ GlobalVariables.v_job_name_list[i]
								+ " jobı hata almıştır. \n");

						GlobalVariables.v_stmt_TERADATA = GlobalVariables.v_conn_TERADATA
								.createStatement();
						GlobalVariables.v_sql = "update bigdata.alarm_params set job_status='OPEN',job_error_detail='"
								+ GlobalVariables.v_job_name_list[i]
								+ " hata almistir.',alarm_time='"
								+ GlobalVariables.v_rs.getString(13)
								+ "', mail_status='1' where job_name='"
								+ GlobalVariables.v_job_name_list[i] + "'";

						GlobalVariables.v_stmt_TERADATA
								.executeUpdate(GlobalVariables.v_sql);

					}

				}

			}
			if (GlobalVariables.v_plan_name_list[i].contains("COGNOS")) {

				// System.out.println("Bu joblar cognos plani jobidir.");

				GlobalVariables.v_sql = "select * from sch2.sch_plan_jobs_v3 where job_name='"
						+ GlobalVariables.v_job_name_list[i]
						+ "' and ett_date=to_date('"
						+ GlobalVariables.v_cognos_ett_date
						+ "','dd.mm.yyyy') and plan_name='"
						+ GlobalVariables.v_plan_name_list[i] + "'";

				// System.out.println(GlobalVariables.v_sql);

				GlobalVariables.v_rs = GlobalVariables.v_stmt_SCHDTF
						.executeQuery(GlobalVariables.v_sql);

				while (GlobalVariables.v_rs.next()) {

					if (GlobalVariables.v_rs.getString(15).startsWith("ERROR")) {

						GlobalVariables.v_open_result.append("OPEN|"
								+ GlobalVariables.v_alarm_id_list[i]
								+ "|MAJOR|"
								+ GlobalVariables.v_rs.getString(13) + "|"
								+ GlobalVariables.v_plan_name_list[i] + " planındaki "
								+ GlobalVariables.v_job_name_list[i]
								+ " jobı hata almıştır. \n");

						GlobalVariables.v_stmt_TERADATA = GlobalVariables.v_conn_TERADATA
								.createStatement();
						GlobalVariables.v_sql = "update bigdata.alarm_params set job_status='OPEN',job_error_detail='"
								+ GlobalVariables.v_job_name_list[i]
								+ " hata almistir.',alarm_time='"
								+ GlobalVariables.v_rs.getString(13)
								+ "', mail_status='1' where job_name='"
								+ GlobalVariables.v_job_name_list[i] + "'";

						GlobalVariables.v_stmt_TERADATA
								.executeUpdate(GlobalVariables.v_sql);

					}

				}

			}
			if (GlobalVariables.v_plan_name_list[i]
					.contains("MASSIVE_RECURSIVE")) {


				GlobalVariables.v_sql = "select * from sch2.sch_plan_jobs_v3 where job_name='"
						+ GlobalVariables.v_job_name_list[i]
						+ "' and ett_date=to_date('"
						+ GlobalVariables.v_massive_recursive_ett_date
						+ "','dd.mm.yyyy') and plan_name='"
						+ GlobalVariables.v_plan_name_list[i] + "'";

				//System.out.println(GlobalVariables.v_sql);

				GlobalVariables.v_rs = GlobalVariables.v_stmt_SCHDTF
						.executeQuery(GlobalVariables.v_sql);

				while (GlobalVariables.v_rs.next()) {

					if (GlobalVariables.v_rs.getString(15).startsWith("ERROR")) {

						GlobalVariables.v_open_result.append("OPEN|"
								+ GlobalVariables.v_alarm_id_list[i]
								+ "|MAJOR|"
								+ GlobalVariables.v_rs.getString(13) + "|"
								+ GlobalVariables.v_plan_name_list[i] + " planındaki "
								+ GlobalVariables.v_job_name_list[i]
								+ " jobı hata almıştır. \n");

						GlobalVariables.v_stmt_TERADATA = GlobalVariables.v_conn_TERADATA
								.createStatement();
						GlobalVariables.v_sql = "update bigdata.alarm_params set job_status='OPEN',job_error_detail='"
								+ GlobalVariables.v_job_name_list[i]
								+ " hata almistir.',alarm_time='"
								+ GlobalVariables.v_rs.getString(13)
								+ "', mail_status='1' where job_name='"
								+ GlobalVariables.v_job_name_list[i] + "'";

						GlobalVariables.v_stmt_TERADATA
								.executeUpdate(GlobalVariables.v_sql);

					}

				}
			}
			if (GlobalVariables.v_plan_name_list[i]
					.contains("COGNOS_RECURSIVE")) {
				// System.out.println("Bu joblar cognos_recursive plani jobidir.");

				GlobalVariables.v_sql = "select * from sch2.sch_plan_jobs_v3 where job_name='"
						+ GlobalVariables.v_job_name_list[i]
						+ "' and ett_date=to_date('"
						+ GlobalVariables.v_cognos_recursive_ett_date
						+ "','dd.mm.yyyy') and plan_name='"
						+ GlobalVariables.v_plan_name_list[i] + "'";

				// System.out.println(GlobalVariables.v_sql);

				GlobalVariables.v_rs = GlobalVariables.v_stmt_SCHDTF
						.executeQuery(GlobalVariables.v_sql);

				while (GlobalVariables.v_rs.next()) {

					if (GlobalVariables.v_rs.getString(15).startsWith("ERROR")) {

						GlobalVariables.v_open_result.append("OPEN|"
								+ GlobalVariables.v_alarm_id_list[i]
								+ "|MAJOR|"
								+ GlobalVariables.v_rs.getString(13) + "|"
								+ GlobalVariables.v_plan_name_list[i] + " planındaki "
								+ GlobalVariables.v_job_name_list[i]
								+ " jobı hata almıştır. \n");

						GlobalVariables.v_stmt_TERADATA = GlobalVariables.v_conn_TERADATA
								.createStatement();
						GlobalVariables.v_sql = "update bigdata.alarm_params set job_status='OPEN',job_error_detail='"
								+ GlobalVariables.v_job_name_list[i]
								+ " hata almistir.',alarm_time='"
								+ GlobalVariables.v_rs.getString(13)
								+ "', mail_status='1' where job_name='"
								+ GlobalVariables.v_job_name_list[i] + "'";

						GlobalVariables.v_stmt_TERADATA
								.executeUpdate(GlobalVariables.v_sql);

					}

				}
			}

		}

		
		GlobalVariables.v_stmt_TERADATA.close();
		GlobalVariables.v_stmt_SCHDTF.close();

		if (GlobalVariables.v_open_result.toString().length() > 0) {
		//	System.out.println(GlobalVariables.v_open_result.toString());
			GlobalVariables.SendMail("DWH Alarm Open Jobs",
					GlobalVariables.v_open_result.toString());

		}

		/****************************************************************
		 * 
		 * bigdata.alarm_params tablosunda statüsü OPEN olan jobların listesini
		 * alır.
		 * 
		 *****************************************************************/
		
		// Bu kısımda statüsü open olan tüm datalar çekilmeli teradata dan yöntem bu olmalı
		
		GlobalVariables.v_stmt_TERADATA = GlobalVariables.v_conn_TERADATA
				.createStatement();
		GlobalVariables.v_sql = "select job_name from bigdata.alarm_params where plan_name='MASSIVE' "
				+ "and job_status='OPEN'";
		GlobalVariables.v_rs = GlobalVariables.v_stmt_TERADATA
				.executeQuery(GlobalVariables.v_sql);

		while (GlobalVariables.v_rs.next()) {

			GlobalVariables.v_alarm_open_list = GlobalVariables.v_alarm_open
					.append(GlobalVariables.v_rs.getString(1)).append(",")
					.toString().split(",");

			GlobalVariables.v_stmt_SCHDTF = GlobalVariables.v_conn_SCHDTF
					.createStatement();

			for (int i = 0; i < GlobalVariables.v_alarm_open_list.length; i++) {

				GlobalVariables.v_sql = "select * from sch2.sch_plan_jobs_v3 where job_name='"
						+ GlobalVariables.v_alarm_open_list[i]
						+ "' and ett_date=to_date('"
						+ GlobalVariables.v_massive_ett_date
						+ "','dd.mm.yyyy')";

				GlobalVariables.v_rs = GlobalVariables.v_stmt_SCHDTF
						.executeQuery(GlobalVariables.v_sql);

				while (GlobalVariables.v_rs.next()) {

					if (!GlobalVariables.v_rs.getString(15).startsWith("ERROR")) {

						GlobalVariables.v_alarm_clear_list = GlobalVariables.v_alarm_clear
								.append(GlobalVariables.v_rs.getString(5))
								.append(",").toString().split(",");

						/****************************************************************
						 * 
						 * v_alarm_open_list dizi değişkenindeki jobların
						 * durumlarını bigdata.alarm_params tablosunda update
						 * eder.
						 * 
						 *****************************************************************/

						GlobalVariables.v_stmt_TERADATA = GlobalVariables.v_conn_TERADATA
								.createStatement();

						for (int a = 0; a < GlobalVariables.v_alarm_clear_list.length; a++) {

							GlobalVariables.v_sql = "update bigdata.alarm_params set job_status='CLEAR',mail_status=0 where job_name='"
									+ GlobalVariables.v_alarm_clear_list[a]
									+ "'";

							GlobalVariables.v_clear_result
									.append(GlobalVariables.v_rs.getString(12)
											+ " "
											+ GlobalVariables.v_alarm_clear_list[a]
											+ " jobı clear olmustur.");
							GlobalVariables.v_stmt_TERADATA
									.executeUpdate(GlobalVariables.v_sql);

						}

						if (GlobalVariables.v_clear_result.toString().length() > 0) {
							
					

							System.out.println(GlobalVariables.v_clear_result
									.toString());
							GlobalVariables.SendMail("DWH Alarm Clear Jobs",
									GlobalVariables.v_clear_result.toString());
							

						}

						GlobalVariables.v_stmt_TERADATA.close();

					}
				}

			}

		}

		GlobalVariables.v_stmt_TERADATA.close();

		GlobalVariables.v_conn_SCHDTF.close();
		GlobalVariables.v_conn_TERADATA.close();

	}

	public static void getSchETLPlans() throws Exception {

		GlobalVariables.v_conn_SCHETL   =    GlobalVariables.getSCHETLConnection();
		GlobalVariables.v_conn_TERADATA =    GlobalVariables.getTERADATAConnection();

		/****************************************************************
		 * 
		 * Son kurulan massive planının ett_date ini alır
		 * 
		 *****************************************************************/

		GlobalVariables.v_stmt_SCHETL = GlobalVariables.v_conn_SCHETL
				.createStatement();

		GlobalVariables.v_sql = "select to_char(max(ETT_DATE),'dd.mm.yyyy') from sch2.sch_plan_jobs_v3 where plan_name='RECURSIVE'";
		GlobalVariables.v_rs = GlobalVariables.v_stmt_SCHETL
				.executeQuery(GlobalVariables.v_sql);
		while (GlobalVariables.v_rs.next()) {
			GlobalVariables.v_recursive_ett_date = GlobalVariables.v_rs
					.getString(1);
		}
		GlobalVariables.v_stmt_SCHETL.close();

		System.out.println("Recursive Etl Date: "
				+ GlobalVariables.v_recursive_ett_date);

	}

}
