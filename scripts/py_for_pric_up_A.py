# pip install pandas  numpy pyreadstat SASPy 
import subprocess
import tushare as ts 
import pandas as pd
import time
import datetime
import os 

def split_dates_by_month(start_date_str, end_date_str,days_str):
    """
    ��ָ����ֹ���ڵ�ʱ�ΰ��»���Ϊ���ʱ��

    ����:
    start_date_str (str): ��ʼ�����ַ�������ʽΪ 'yyyymmdd'
    end_date_str (str): ���������ַ�������ʽΪ 'yyyymmdd'

    ����:
    list: �������ʱ�ε��б�ÿ��ʱ��Ϊһ��Ԫ�飬��ʽΪ (��ʼ�����ַ���, ���������ַ���)
    """
    start_date = datetime.datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.datetime.strptime(end_date_str, '%Y%m%d')
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        next_month = current_date.replace(day=1) + datetime.timedelta(days=days_str)
        next_month = next_month.replace(day=1)
        if next_month > end_date:
            date_list.append((current_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d')))
            break
        date_list.append((current_date.strftime('%Y%m%d'), (next_month - datetime.timedelta(days=1)).strftime('%Y%m%d')))
        current_date = next_month
    return date_list

# ����ts_dwload_daily�������������ظ��ɵ�daily����
def  ts_dwload_daily(ts_table,file_dir,is_update_all,start_date,end_date,is_tslst_src_online,mkt,visit_per_min):
	if mkt==2:# mkt=1 ΪH���г�
		# ��ȡ����H�ɵ��б�,������ڱ���stock_codes��
		file_path = os.path.join(data_dir, 'stock_hk_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.hk_basic()
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
	elif mkt==3:# mkt=3 Ϊ�����г�
		# ��ȡ����H�ɵ��б�,������ڱ���stock_codes��
		file_path = os.path.join(data_dir, 'stock_us_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.us_basic()
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
	else:# mkt=1��� ΪA���г�
		# ��ȡ����A�ɵ��б�,������ڱ���stock_codes��
		file_path = os.path.join(data_dir, 'stock_cn_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
						
		
	stock_codes = stock_basic_info['ts_code'].unique()	
	
	#�����������ͼ�ʱ������1�����ڷ���Ƶ�ʹ���ʱ��sleepһ��ʱ��
	start_time = datetime.datetime.now()
	rd_cnt=0
	# by ��Ʊid ѭ������
	for code in stock_codes:
		rd_cnt=rd_cnt+1
		sec_round=(datetime.datetime.now()-start_time).total_seconds()
		
		if sec_round<60 and rd_cnt<visit_per_min:
			print(f"Downloading data for {ts_table} - {code} ע�������ݱ�1�������ѷ��ʴ���={rd_cnt}")
			try:
				if is_update_all==1:
					#A����������
					if ts_table in ('daily'):
						df = pro.daily(ts_code=code)
				else:
					#A����������
					if ts_table in ('daily'):
						df = pro.daily(ts_code=code,start_date=start_date, end_date=end_date)
				file_path = os.path.join(file_dir, f"{ts_table}_{code}.csv")
				df.to_csv(file_path, index=False,encoding='utf-8')	
			except Exception as e:
				print(f"Error downloading data for stock {code}: {e}")
				
		elif sec_round<60 and rd_cnt>=visit_per_min:
			sec_to_sleep=60-sec_round
			print(f"1�����ڷ���Ƶ�ʹ���,��ͣ{sec_to_sleep}��")
			time.sleep(sec_to_sleep)  # ��ͣ X ��
			print("��������")
			
		elif sec_round>=60:
			print("���¼������Ƶ��")
			start_time = datetime.datetime.now()
			rd_cnt=0
						
	if is_update_all==1:
		os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_tdr.csv")
	else:
		os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_tdr_{start_date}_{end_date}.csv")
	
	os.system(f"del {file_dir}\\*{ts_table}*.csv")
	
	print(f"The Data Download task of {ts_table} finished")
	
# ����ts_dwload_index_daily��������������index����
def  ts_dwload_index_daily(ts_table,file_dir,is_update_all,start_date,end_date,is_tslst_src_online,visit_per_min):
	# ��ȡ����ָ�����б�,������ڱ���index_codes��
	file_path = os.path.join(data_dir, 'index_basic.csv') 
	if is_tslst_src_online==1:
		index_basic = pro.index_basic()
		index_basic.to_csv(file_path, index=False,encoding='utf-8')
	else:
		index_basic = pd.read_csv(file_path) 
						
		
	index_codes = index_basic['ts_code'].unique()	
	
	#�����������ͼ�ʱ������1�����ڷ���Ƶ�ʹ���ʱ��sleepһ��ʱ��
	start_time = datetime.datetime.now()
	rd_cnt=0
	# by ��Ʊid ѭ������
	for code in index_codes:
		rd_cnt=rd_cnt+1
		sec_round=(datetime.datetime.now()-start_time).total_seconds()
		
		if sec_round<60 and rd_cnt<visit_per_min:
			print(f"Downloading data for {ts_table} - {code} ע�������ݱ�1�������ѷ��ʴ���={rd_cnt}")
			try:
				if is_update_all==1:
					#A����������
					if ts_table in ('index_daily'):
						df = pro.index_daily(ts_code=code)
					#����ָ��ÿ��ָ��
					if ts_table in ('index_dailybasic'):
						df = pro.index_dailybasic(ts_code=code)
				else:
					#A����������
					if ts_table in ('index_daily'):
						df = pro.index_daily(ts_code=code,start_date=start_date, end_date=end_date)
				file_path = os.path.join(file_dir, f"{ts_table}_{code}.csv")
				df.to_csv(file_path, index=False,encoding='utf-8')	
			except Exception as e:
				print(f"Error downloading data for index {code}: {e}")
				
		elif sec_round<60 and rd_cnt>=visit_per_min:
			sec_to_sleep=60-sec_round
			print(f"1�����ڷ���Ƶ�ʹ���,��ͣ{sec_to_sleep}��")
			time.sleep(sec_to_sleep)  # ��ͣ X ��
			print("��������")
			
		elif sec_round>=60:
			print("���¼������Ƶ��")
			start_time = datetime.datetime.now()
			rd_cnt=0
						
	if is_update_all==1:
		if ts_table in ('index_daily'):
			os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_tdr.csv")
		else:
			os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}.csv")
	else:
		if ts_table in ('index_daily'):
			os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_tdr_{start_date}_{end_date}.csv")
		else:
			os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_{start_date}_{end_date}.csv")
		
	os.system(f"del {file_dir}\\*{ts_table}*.csv")
	print(f"The Data Download task of {ts_table} finished")

# ����ts_dwload_bytsid���������ڴ�tushare by ��Ʊid �������ظ���ԭʼ����
def ts_dwload_bytsid(ts_table,file_dir,is_update_all,start_date,end_date,is_tslst_src_online,mkt,visit_per_min):
	if mkt==2:# mkt=1 ΪH���г�
		# ��ȡ����H�ɵ��б�,������ڱ���stock_codes��
		file_path = os.path.join(data_dir, 'stock_hk_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.hk_basic()
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
	elif mkt==3:# mkt=3 Ϊ�����г�
		# ��ȡ����H�ɵ��б�,������ڱ���stock_codes��
		file_path = os.path.join(data_dir, 'stock_us_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.us_basic()
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
	elif mkt==4:# mkt=4 Ϊ��ļ����
		# ��ȡ���й�ļ������б�,������ڱ���stock_codes��
		file_path = os.path.join(data_dir, 'fund_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.fund_basic(market='E')
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
	else:# mkt=1��� ΪA���г�
		# ��ȡ����A�ɵ��б�,������ڱ���stock_codes��
		file_path = os.path.join(data_dir, 'stock_cn_basic_info.csv') 
		if is_tslst_src_online==1:
			stock_basic_info = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
			stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
		else:
			stock_basic_info = pd.read_csv(file_path) 
						
		
	stock_codes = stock_basic_info['ts_code'].unique()	
	
	#�����������ͼ�ʱ������1�����ڷ���Ƶ�ʹ���ʱ��sleepһ��ʱ��
	start_time = datetime.datetime.now()
	rd_cnt=0
	# by ��Ʊid ѭ������
	for code in stock_codes:
		rd_cnt=rd_cnt+1
		sec_round=(datetime.datetime.now()-start_time).total_seconds()
		
		if sec_round<60 and rd_cnt<visit_per_min:
			print(f"Downloading data for {ts_table} - {code} ע�������ݱ�1�������ѷ��ʴ���={rd_cnt}")
			try:
				if is_update_all==1:
					#A����������
					if ts_table in ('daily'):
						df = pro.daily(ts_code=code)
					#ÿ��ָ��
					if ts_table in ('daily_basic'):
						df = pro.daily_basic(ts_code=code)
					#��Ȩ����
					if ts_table in ('adj_factor'):
						df = pro.adj_factor(ts_code=code)
					#�����ʽ�����
					if ts_table in ('moneyflow'):
						df = pro.moneyflow(ts_code=code)
					#ÿ���ǵ�ͣ�۸�
					if ts_table in ('stk_limit'):
						df = pro.stk_limit(ts_code=code)
					#����������
					if ts_table in ('fina_audit'):
						df = pro.fina_audit(ts_code=code)
					#����ָ������
					if ts_table in ('fina_indicator'):
						df = pro.fina_indicator(ts_code=code,fields='ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag')
					#�����
					if ts_table in ('income'):
						df = pro.income(ts_code=code)
					#�ʲ���ծ��
					if ts_table in ('balancesheet'):
						df = pro.balancesheet(ts_code=code)
					#�ֽ�������
					if ts_table in ('cashflow'):
						df = pro.cashflow(ts_code=code)
					#ҵ��Ԥ��
					if ts_table in ('forecast'):
						df = pro.forecast(ts_code=code)
					#ҵ���챨
					if ts_table in ('express'):
						df = pro.express(ts_code=code)
					#�ֺ��͹�
					if ts_table in ('dividend'):
						df = pro.dividend(ts_code=code)
					#��Ӫҵ�񹹳�
					if ts_table in ('fina_mainbz'):
						df = pro.fina_mainbz(ts_code=code)
					#����ӯ��Ԥ������
					if ts_table in ('report_rc'):
						df = pro.report_rc(ts_code=code)
					#ÿ�ճ��뼰ʤ�� �������5000�������Է�ҳ����ѭ����ȡ
					if ts_table in ('cyq_perf'):
						df = pro.cyq_perf(ts_code=code)
					#ÿ�ճ���ֲ�,�������������2000�������԰���Ʊ���������ѭ����ȡ
					if ts_table in ('cyq_chips'):
						df = pro.cyq_chips(ts_code=code)
					#��Ʊ�������ӣ��������ӣ�
					if ts_table in ('stk_factor'):
						df = pro.stk_factor(ts_code=code)
					#��Ʊ����������(רҵ��)
					if ts_table in ('stk_factor_pro'):
						df = pro.stk_factor_pro(ts_code=code)
					#�������ϵͳ�ֹɻ���
					if ts_table in ('ccass_hold'):
						df = pro.ccass_hold(ts_code=code)
					#�������ϵͳ�ֹ���ϸ
					if ts_table in ('ccass_hold_detail'):
						df = pro.ccass_hold_detail(ts_code=code)
					#��ļ����ֲ�����
					if ts_table in ('fund_portfolio'):
						df = pro.fund_portfolio(ts_code=code)
					#����۹�ͨ�ֹ���ϸ
					if ts_table in ('hk_hold'):
						df = pro.hk_hold(ts_code=code)
					#�۹�����
					if ts_table in ('hk_daily'):
						df = pro.hk_daily(ts_code=code)
					#�۹ɷ������� �������������8000�����ݣ�����ͨ����Ʊ���������ѭ����ȡ
					if ts_table in ('hk_mins'):
						df = pro.hk_mins(ts_code=code)
					#�������� �������������6000�����ݣ��ɸ������ڲ���ѭ����ȡ����ͨ��ʽȨ�޺�Ҳ��֧�ַ�ҳ��ȡȫ����ʷ
					if ts_table in ('us_daily'):
						df = pro.us_daily(ts_code=code)
				else:
					#A����������
					if ts_table in ('daily'):
						df = pro.daily(ts_code=code,start_date=start_date, end_date=end_date)
					#ÿ��ָ��
					if ts_table in ('daily_basic'):
						df = pro.daily_basic(ts_code=code,start_date=start_date, end_date=end_date)
					#��Ȩ����
					if ts_table in ('adj_factor'):
						df = pro.adj_factor(ts_code=code,start_date=start_date, end_date=end_date)
					#�����ʽ�����
					if ts_table in ('moneyflow'):
						df = pro.moneyflow(ts_code=code,start_date=start_date, end_date=end_date)
					#ÿ���ǵ�ͣ�۸�
					if ts_table in ('stk_limit'):
						df = pro.stk_limit(ts_code=code,start_date=start_date, end_date=end_date)
					#����������
					if ts_table in ('fina_audit'):
						df = pro.fina_audit(ts_code=code,start_date=start_date, end_date=end_date)
					#����ָ������
					if ts_table in ('fina_indicator'):
						df = pro.fina_indicator(ts_code=code,fields='ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag',start_date=start_date, end_date=end_date)
					#�����
					if ts_table in ('income'):
						df = pro.income(ts_code=code,start_date=start_date, end_date=end_date)
					#�ʲ���ծ��
					if ts_table in ('balancesheet'):
						df = pro.balancesheet(ts_code=code,start_date=start_date, end_date=end_date)
					#�ֽ�������
					if ts_table in ('cashflow'):
						df = pro.cashflow(ts_code=code,start_date=start_date, end_date=end_date)
					#ҵ��Ԥ��
					if ts_table in ('forecast'):
						df = pro.forecast(ts_code=code,start_date=start_date, end_date=end_date)
					#ҵ���챨
					if ts_table in ('express'):
						df = pro.express(ts_code=code,start_date=start_date, end_date=end_date)
					#�ֺ��͹�
					if ts_table in ('dividend'):
						df = pro.dividend(ts_code=code,start_date=start_date, end_date=end_date)
					#��Ӫҵ�񹹳�
					if ts_table in ('fina_mainbz'):
						df = pro.fina_mainbz(ts_code=code,start_date=start_date, end_date=end_date)
					#����ӯ��Ԥ������
					if ts_table in ('report_rc'):
						df = pro.report_rc(ts_code=code,start_date=start_date, end_date=end_date)
					#ÿ�ճ��뼰ʤ�� �������5000�������Է�ҳ����ѭ����ȡ
					if ts_table in ('cyq_perf'):
						df = pro.cyq_perf(ts_code=code,start_date=start_date, end_date=end_date)
					#ÿ�ճ���ֲ�,�������������2000�������԰���Ʊ���������ѭ����ȡ
					if ts_table in ('cyq_chips'):
						df = pro.cyq_chips(ts_code=code,start_date=start_date, end_date=end_date)
					#��Ʊ�������ӣ��������ӣ�
					if ts_table in ('stk_factor'):
						df = pro.stk_factor(ts_code=code,start_date=start_date, end_date=end_date)
					#��Ʊ����������(רҵ��)
					if ts_table in ('stk_factor_pro'):
						df = pro.stk_factor_pro(ts_code=code,start_date=start_date, end_date=end_date)	
					#�������ϵͳ�ֹɻ���
					if ts_table in ('ccass_hold'):
						df = pro.ccass_hold(ts_code=code,start_date=start_date, end_date=end_date)
					#�������ϵͳ�ֹ���ϸ
					if ts_table in ('ccass_hold_detail'):
						df = pro.ccass_hold_detail(ts_code=code,start_date=start_date, end_date=end_date)
					#��ļ����ֲ�����
					if ts_table in ('fund_portfolio'):
						df = pro.fund_portfolio(ts_code=code,start_date=start_date, end_date=end_date)
					#����۹�ͨ�ֹ���ϸ
					if ts_table in ('hk_hold'):
						df = pro.hk_hold(ts_code=code,start_date=start_date, end_date=end_date)					
					#�۹�����
					if ts_table in ('hk_daily'):
						df = pro.hk_daily(ts_code=code,start_date=start_date, end_date=end_date)
					#�۹ɷ������� �������������8000�����ݣ�����ͨ����Ʊ���������ѭ����ȡ
					if ts_table in ('hk_mins'):
						df = pro.hk_mins(ts_code=code,start_date=start_date, end_date=end_date)
					#�������� �������������6000�����ݣ��ɸ������ڲ���ѭ����ȡ����ͨ��ʽȨ�޺�Ҳ��֧�ַ�ҳ��ȡȫ����ʷ
					if ts_table in ('us_daily'):
						df = pro.us_daily(ts_code=code,start_date=start_date, end_date=end_date)
					
				file_path = os.path.join(file_dir, f"{ts_table}_{code}.csv")
				df.to_csv(file_path, index=False,encoding='utf-8')
					
			except Exception as e:
				print(f"Error downloading data for stock {code}: {e}")
				
		elif sec_round<60 and rd_cnt>=visit_per_min:
			sec_to_sleep=60-sec_round
			print(f"1�����ڷ���Ƶ�ʹ���,��ͣ{sec_to_sleep}��")
			time.sleep(sec_to_sleep)  # ��ͣ X ��
			print("��������")
			
		elif sec_round>=60:
			print("���¼������Ƶ��")
			start_time = datetime.datetime.now()
			rd_cnt=0
						
	if is_update_all==1:
		os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}.csv")
	else:
		os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_{start_date}_{end_date}.csv")
		
	os.system(f"del {file_dir}\\*{ts_table}*.csv")
	print(f"The Data Download task of {ts_table} finished")

# ����ts_dwload_bydate����������by trade_date�������ݣ� ������������
def  ts_dwload_bydate(ts_table,file_dir,start_date,end_date,visit_per_min):
	# ��ȡ����A�ɽ����յ��б�,������ڱ���trade_date_lst��
	df=pro.trade_cal(exchange='', start_date=start_date, end_date=end_date,is_open='1',fields='cal_date')			
	trade_date_lst = df['cal_date'].unique()	
	
	#�����������ͼ�ʱ������1�����ڷ���Ƶ�ʹ���ʱ��sleepһ��ʱ��
	start_time = datetime.datetime.now()
	rd_cnt=0
	# by trade_date ѭ������
	for tdate in trade_date_lst:
		rd_cnt=rd_cnt+1
		sec_round=(datetime.datetime.now()-start_time).total_seconds()
		if sec_round<60 and rd_cnt<visit_per_min:
			print(f"Downloading data for {ts_table} - {tdate} ע�������ݱ�1�������ѷ��ʴ���={rd_cnt}")
			try:
				#������ÿ����ϸ
				if ts_table in ('top_list'):
					df = pro.top_list(trade_date=tdate)
				#�����������ϸ
				if ts_table in ('top_inst'):
					df = pro.top_inst(trade_date=tdate)
				file_path = os.path.join(file_dir, f"{ts_table}_{tdate}.csv")
				df.to_csv(file_path, index=False,encoding='utf-8')	
			except Exception as e:
				print(f"Error downloading data for stock {tdate}: {e}")
		elif sec_round<60 and rd_cnt>=visit_per_min:
			sec_to_sleep=60-sec_round
			print(f"1�����ڷ���Ƶ�ʹ���,��ͣ{sec_to_sleep}��")
			time.sleep(sec_to_sleep)  # ��ͣ X ��
			print("��������")
		elif sec_round>=60:
			print("���¼������Ƶ��")
			start_time = datetime.datetime.now()
			rd_cnt=0
	# ѭ��������ɺ���ļ��ϲ�����ɨ����
	os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_{start_date}_{end_date}.csv")
	os.system(f"del {file_dir}\\*{ts_table}*.csv")
	print(f"The Data Download task of {ts_table}_{start_date}_{end_date} finished")

		
# ����ts_dwload_bytsid_and_months����������ͬʱ����ts_id��ʱ������������ݣ� �����ֲ�����
def  ts_dwload_bytsid_and_months(ts_table,file_dir,start_date,end_date,is_tslst_src_online,days_str,visit_per_min):
	# ��ȡ����A�ɵ��б�,������ڱ���stock_codes��
	file_path = os.path.join(data_dir, 'stock_cn_basic_info.csv') 
	if is_tslst_src_online==1:
		stock_basic_info = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
		stock_basic_info.to_csv(file_path, index=False,encoding='utf-8')
	else:
		stock_basic_info = pd.read_csv(file_path)
	stock_codes = stock_basic_info['ts_code'].unique()
	
	# ��ָ����ֹ���ڵ�ʱ�ΰ��»���Ϊ���ʱ�Σ���װ����date_tuples��Ԫ����
	date_tuples = split_dates_by_month(start_date_str=start_date,end_date_str=end_date,days_str=days_str)
	
	#�ڿ�ʼѭ��ǰ�����������ͼ�ʱ������1�����ڷ���Ƶ�ʹ���ʱ��sleepһ��ʱ��
	start_time = datetime.datetime.now()
	rd_cnt=0
	
	# by code ѭ������
	for s_date,e_date in date_tuples:
		for code in stock_codes:
			rd_cnt=rd_cnt+1
			sec_round=(datetime.datetime.now()-start_time).total_seconds()
			
			# ��������Ƶ�����ƿ��
			if sec_round<60 and rd_cnt<visit_per_min:
				print(f"Downloading data for {ts_table}_{code}_{s_date}_{e_date}, ע�������ݱ�1�������ѷ��ʴ���={rd_cnt}")
				try:
					#ÿ�ճ���ֲ�
					if ts_table in ('cyq_chips'):
						df = pro.cyq_chips(ts_code=code, start_date=s_date, end_date=e_date)
					#ÿ�ճ��뼰ʤ��
					if ts_table in ('cyq_perf'):
						df = pro.cyq_perf(ts_code=code, start_date=s_date, end_date=e_date)
					#��Ʊ����������(רҵ��)
					if ts_table in ('stk_factor_pro'):
						df = pro.stk_factor_pro(ts_code=code, start_date=s_date, end_date=e_date)
					#��Ʊ�������ӣ��������ӣ�
					if ts_table in ('stk_factor'):
						df = pro.stk_factor(ts_code=code, start_date=s_date, end_date=e_date)
					#A����������
					if ts_table in ('daily'):
						df = pro.daily(ts_code=code,start_date=s_date, end_date=e_date)
					#ÿ��ָ��
					if ts_table in ('daily_basic'):
						df = pro.daily_basic(ts_code=code,start_date=s_date, end_date=e_date)
					#��Ȩ����
					if ts_table in ('adj_factor'):
						df = pro.adj_factor(ts_code=code,start_date=s_date, end_date=e_date)
					#�����ʽ�����
					if ts_table in ('moneyflow'):
						df = pro.moneyflow(ts_code=code,start_date=s_date, end_date=e_date)
					#ÿ���ǵ�ͣ�۸�
					if ts_table in ('stk_limit'):
						df = pro.stk_limit(ts_code=code,start_date=s_date, end_date=e_date)
					#����������
					if ts_table in ('fina_audit'):
						df = pro.fina_audit(ts_code=code,start_date=s_date, end_date=e_date)
					#����ָ������
					if ts_table in ('fina_indicator'):
						df = pro.fina_indicator(ts_code=code,fields='ts_code,ann_date,end_date,eps,dt_eps,total_revenue_ps,revenue_ps,capital_rese_ps,surplus_rese_ps,undist_profit_ps,extra_item,profit_dedt,gross_margin,current_ratio,quick_ratio,cash_ratio,invturn_days,arturn_days,inv_turn,ar_turn,ca_turn,fa_turn,assets_turn,op_income,valuechange_income,interst_income,daa,ebit,ebitda,fcff,fcfe,current_exint,noncurrent_exint,interestdebt,netdebt,tangible_asset,working_capital,networking_capital,invest_capital,retained_earnings,diluted2_eps,bps,ocfps,retainedps,cfps,ebit_ps,fcff_ps,fcfe_ps,netprofit_margin,grossprofit_margin,cogs_of_sales,expense_of_sales,profit_to_gr,saleexp_to_gr,adminexp_of_gr,finaexp_of_gr,impai_ttm,gc_of_gr,op_of_gr,ebit_of_gr,roe,roe_waa,roe_dt,roa,npta,roic,roe_yearly,roa2_yearly,roe_avg,opincome_of_ebt,investincome_of_ebt,n_op_profit_of_ebt,tax_to_ebt,dtprofit_to_profit,salescash_to_or,ocf_to_or,ocf_to_opincome,capitalized_to_da,debt_to_assets,assets_to_eqt,dp_assets_to_eqt,ca_to_assets,nca_to_assets,tbassets_to_totalassets,int_to_talcap,eqt_to_talcapital,currentdebt_to_debt,longdeb_to_debt,ocf_to_shortdebt,debt_to_eqt,eqt_to_debt,eqt_to_interestdebt,tangibleasset_to_debt,tangasset_to_intdebt,tangibleasset_to_netdebt,ocf_to_debt,ocf_to_interestdebt,ocf_to_netdebt,ebit_to_interest,longdebt_to_workingcapital,ebitda_to_debt,turn_days,roa_yearly,roa_dp,fixed_assets,profit_prefin_exp,non_op_profit,op_to_ebt,nop_to_ebt,ocf_to_profit,cash_to_liqdebt,cash_to_liqdebt_withinterest,op_to_liqdebt,op_to_debt,roic_yearly,total_fa_trun,profit_to_op,q_opincome,q_investincome,q_dtprofit,q_eps,q_netprofit_margin,q_gsprofit_margin,q_exp_to_sales,q_profit_to_gr,q_saleexp_to_gr,q_adminexp_to_gr,q_finaexp_to_gr,q_impair_to_gr_ttm,q_gc_to_gr,q_op_to_gr,q_roe,q_dt_roe,q_npta,q_opincome_to_ebt,q_investincome_to_ebt,q_dtprofit_to_profit,q_salescash_to_or,q_ocf_to_sales,q_ocf_to_or,basic_eps_yoy,dt_eps_yoy,cfps_yoy,op_yoy,ebt_yoy,netprofit_yoy,dt_netprofit_yoy,ocf_yoy,roe_yoy,bps_yoy,assets_yoy,eqt_yoy,tr_yoy,or_yoy,q_gr_yoy,q_gr_qoq,q_sales_yoy,q_sales_qoq,q_op_yoy,q_op_qoq,q_profit_yoy,q_profit_qoq,q_netprofit_yoy,q_netprofit_qoq,equity_yoy,rd_exp,update_flag',start_date=s_date, end_date=e_date)
					#�����
					if ts_table in ('income'):
						df = pro.income(ts_code=code,start_date=s_date, end_date=e_date)
					#�ʲ���ծ��
					if ts_table in ('balancesheet'):
						df = pro.balancesheet(ts_code=code,start_date=s_date, end_date=e_date)
					#�ֽ�������
					if ts_table in ('cashflow'):
						df = pro.cashflow(ts_code=code,start_date=s_date, end_date=e_date)
					#ҵ��Ԥ��
					if ts_table in ('forecast'):
						df = pro.forecast(ts_code=code,start_date=s_date, end_date=e_date)
					#ҵ���챨
					if ts_table in ('express'):
						df = pro.express(ts_code=code,start_date=s_date, end_date=e_date)
					#�ֺ��͹�
					if ts_table in ('dividend'):
						df = pro.dividend(ts_code=code,start_date=s_date, end_date=e_date)
					#��Ӫҵ�񹹳�
					if ts_table in ('fina_mainbz'):
						df = pro.fina_mainbz(ts_code=code,start_date=s_date, end_date=e_date)
					#����ӯ��Ԥ������
					if ts_table in ('report_rc'):
						df = pro.report_rc(ts_code=code,start_date=s_date, end_date=e_date)
					#ÿ�ճ��뼰ʤ�� �������5000�������Է�ҳ����ѭ����ȡ
					if ts_table in ('cyq_perf'):
						df = pro.cyq_perf(ts_code=code,start_date=s_date, end_date=e_date)
					#ÿ�ճ���ֲ�,�������������2000�������԰���Ʊ���������ѭ����ȡ
					if ts_table in ('cyq_chips'):
						df = pro.cyq_chips(ts_code=code,start_date=s_date, end_date=e_date)
					#��Ʊ�������ӣ��������ӣ�
					if ts_table in ('stk_factor'):
						df = pro.stk_factor(ts_code=code,start_date=s_date, end_date=e_date)
					#��Ʊ����������(רҵ��)
					if ts_table in ('stk_factor_pro'):
						df = pro.stk_factor_pro(ts_code=code,start_date=s_date, end_date=e_date)	
					#�������ϵͳ�ֹɻ���
					if ts_table in ('ccass_hold'):
						df = pro.ccass_hold(ts_code=code,start_date=s_date, end_date=e_date)
					#�������ϵͳ�ֹ���ϸ
					if ts_table in ('ccass_hold_detail'):
						df = pro.ccass_hold_detail(ts_code=code,start_date=s_date, end_date=e_date)
					#��ļ����ֲ�����
					if ts_table in ('fund_portfolio'):
						df = pro.fund_portfolio(ts_code=code,start_date=s_date, end_date=e_date)
					#����۹�ͨ�ֹ���ϸ
					if ts_table in ('hk_hold'):
						df = pro.hk_hold(ts_code=code,start_date=s_date, end_date=e_date)	
					file_path = os.path.join(file_dir, f"{ts_table}_{code}_{s_date}_{e_date}.csv")
					df.to_csv(file_path, index=False,encoding='utf-8')	
				except Exception as e:
					print(f"Error downloading data for {ts_table}_{code}_{s_date}_{e_date}: {e}")
			elif sec_round<60 and rd_cnt>=visit_per_min:
				sec_to_sleep=60-sec_round
				print(f"1�����ڷ���Ƶ�ʹ���,��ͣ{sec_to_sleep}��")
				time.sleep(sec_to_sleep)  # ��ͣ X ��
				print("��������")
			elif sec_round>=60:
				print("���¼������Ƶ��")
				start_time = datetime.datetime.now()
				rd_cnt=0
	
	# ѭ��������ɺ���ļ��ϲ�����ɨ����
	os.system(f"copy /b {file_dir}\\*{ts_table}*.csv {data_dir}\\{ts_table}_{start_date}_{end_date}.csv")
	os.system(f"del {file_dir}\\*{ts_table}*.csv")
	print(f"The Data Download task of {ts_table}_{start_date}_{end_date} finished")
		
	
"""
ts_dwload_daily(ts_table='daily',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)

ts_dwload_bytsid(ts_table='daily_basic',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='moneyflow',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='stk_limit',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='adj_factor',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='fina_audit',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='fina_indicator',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='income',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='balancesheet',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='cashflow',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='forecast',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='express',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='dividend',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='report_rc',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='ccass_hold_detail',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='ccass_hold',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='stk_factor',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='stk_factor_pro',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='hk_hold',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=0,mkt=1,visit_per_min=400)
ts_dwload_bytsid(ts_table='fund_portfolio',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=1,mkt=4,visit_per_min=200)


ts_dwload_index_daily(ts_table='index_daily',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=1,visit_per_min=400)
ts_dwload_index_daily(ts_table='index_dailybasic',file_dir=data_dir_daily,is_update_all=1,start_date=start_date,end_date=end_date,is_tslst_src_online=1,visit_per_min=400)

ts_dwload_bydate(ts_table='top_list',file_dir=data_dir_daily,start_date='20000101',end_date='20240531',visit_per_min=400)
ts_dwload_bydate(ts_table='top_inst',file_dir=data_dir_daily,start_date='20000101',end_date='20240531',visit_per_min=400)

ts_dwload_bytsid_and_months(ts_table='cyq_chips',file_dir=data_dir_daily,start_date='20100101'end_date='20240603',is_tslst_src_online=1,days_str=92,visit_per_min=200)
ts_dwload_bytsid_and_months(ts_table='cyq_perf',file_dir=data_dir_daily,start_date='20100101',end_date='20240603',is_tslst_src_online=1,days_str=392,visit_per_min=200)
ts_dwload_bytsid_and_months(ts_table='stk_factor_pro',file_dir=data_dir_daily,start_date='20100101',end_date='20240603',is_tslst_src_online=1,days_str=1832,visit_per_min=30)
ts_dwload_bytsid_and_months(ts_table='stk_factor',file_dir=data_dir_index,start_date='20100101',end_date='20240603',is_tslst_src_online=1,days_str=1832,visit_per_min=30)
ts_dwload_bytsid_and_months(ts_table='daily',file_dir=data_dir_daily,start_date='19980101',end_date='20240603',is_tslst_src_online=1,days_str=7202,visit_per_min=400)
"""