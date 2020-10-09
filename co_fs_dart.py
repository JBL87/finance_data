import pandas as pd
import requests
from datetime import datetime
from glob import glob
import concurrent.futures
import platform
import pantab
import time
import OpenDartReader
import pygsheets
import helper

dt = datetime.now()
suffix = str(dt.year)[2:] + ('0'*len(str(dt.month)) + str(dt.month)) + '0'*len(str(dt.day)) + str(dt.day)
api_key = 'f7ede636b9181ffbb65d83424556b7785d4ad727'
# api_key = 'cfd05b87cced5c6b21c08bc0cb4b6648bb44e0d1'
# api_key = 'ec1b22c70ff704db64e49b4319c7671b76098501'
dart = OpenDartReader(api_key)

save_path = r"C:\Users\bong2\OneDrive\DataArchive\DB_주식관련\DB_주식분석_재무제표\DART에서 받은 재무제표_pickle 파일\\"
corp_list_path = r"C:\Users\bong2\OneDrive\DataArchive\DB_주식관련\DB_주식분석_기업정보\취합완료_상장사기업정보\기업정보취합_final.csv"
finstate_path = r"C:\Users\bong2\OneDrive\DataArchive\DB_주식관련\DB_주식분석_재무제표\DART에서 받은 재무제표_pickle 파일\OpenDartReader//"

credential_path = r'C:\Users\bong2\OneDrive\Python_Codes\client_secret.json'
gc = pygsheets.authorize(client_secret=credential_path, no_cache=True)
gfile = gc.open("DB_기업정보")
code_list = gfile.worksheet_by_title('FS_update_list').get_as_df(numerize=False)
code_list.dropna(subset=['회사코드'], inplace=True)
#------------------------------------------------------------------------------------------------------------
def dart_api_get_fs_data(param):
    '''
    dart api에서 재무제표 데이터 가져오기
    '''
    report_frequency, bsns_year, corp_code, co_name = param
    report_dict = {'1분기': '11013',
                    '반기': '11012',
                    '3분기': '11014',
                    '연간': '11011'}
    url = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'
    df = pd.DataFrame()
    fs_div = ['CFS', 'OFS']  # "CFS":연결재무제표, "OFS":재무제표
    for fs_type in fs_div:
        params = {'crtfc_key': api_key,
                  'corp_code': corp_code,
                  'bsns_year':  bsns_year,   # 사업년도
                  'reprt_code': report_dict[report_frequency],
                  'fs_div': fs_type}
        r = requests.get(url, params=params)
        if r.json()['status'] == '000':
            temp_df = pd.DataFrame(r.json()['list'])
            if fs_type == 'CFS':
                temp_df.insert(0, '개별/연결', '연결')  # 개별/연결 컬럼 추가
            else:
                temp_df.insert(0, '개별/연결', '개별')  # 개별/연결 컬럼 추가
            temp_df.insert(0, '주기', report_frequency)  # 주기 컬럼 추가
            df = df.append(temp_df)
        else:
            if fs_type == 'CFS':
                print(f'{co_name} {bsns_year} {report_frequency} 연결 없음')
            else:
                print(f'{co_name} {bsns_year} {report_frequency} 개별 없음')
    if len(df) > 0:
        df.to_pickle(save_path + f'DartAPI_{co_name}_{bsns_year}년_{report_frequency}.pkl')
        print(f'{co_name} {bsns_year} {report_frequency} 완료')
        time.sleep(1.5)
    else:
        print(f'{co_name} {bsns_year} {report_frequency} 전체 없음')
        time.sleep(1)
#------------------------------------------------------------------------------------------------------------
def dart_api_get_all_fs(bsns_year, report_frequency):
    '''
    dart api에서 재무제표 데이터 가져오기
    bsns_year = 사업년도 4자리
    report_frequency = 1분기 / 반기 / 3분기 / 연간
    '''
    start_time_total = dt.now()
    corp_list = code_list[['회사코드', '종목명']]
    corp_list.insert(0, 'year', bsns_year)
    corp_list.insert(0, 'tp', report_frequency)
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(dart_api_get_fs_data, corp_list.values.tolist())
    print('(총 소요시간: ' + str(dt.now() - start_time_total)+")")
#------------------------------------------------------------------------------------------------------------
def dart_api_get_employee_data(bsns_year, report_frequency):
    start_time_total = dt.now()
    report_dict = {'1분기': '11013',
                   '반기': '11012',
                   '3분기': '11014',
                   '연간': '11011'}
    corp_list = code_list[['회사코드']]
    corp_list.insert(0, 'year', bsns_year)
    corp_list.insert(0, 'tp', report_dict[report_frequency])
    df = pd.DataFrame()
    url = 'https://opendart.fss.or.kr/api/empSttus.json'
    def get_data(data): # 단일회사의 직원현황 가져오는 함수
        tp, year, code = data
        params = {'crtfc_key': api_key,
                  'corp_code': code,
                  'bsns_year':  year, # 사업년도
                  'reprt_code': tp}  # 1분기/반기/3분기/연간
        r = requests.get(url, params=params)
        try:
            temp_df = pd.DataFrame(r.json()['list'])
            temp_df.insert(0, 'bsnns_year', year)
            temp_df.insert(0, '주기', tp)  # 주기 컬럼 추가
            return temp_df
        except:
            pass
        time.sleep(2)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        result = executor.map(get_data, corp_list.values.tolist())
    df = pd.concat([df for df in result], axis=0).reset_index(drop=True)
    df.to_pickle(save_path + f'DartAPI_{bsns_year}년_{report_frequency}_직원현황.pkl')
    print('(총 소요시간: ' + str(dt.now() - start_time_total)+")")
#------------------------------------------------------------------------------------------------------------
def union_n_extract_fs(year, report_type):  # dart api로 받은거 1차 취합. 년도와 연간/반기/3분기/1분기 넣기
    '''
    dart api로 받은 데이터 1차 취합
    '''
    start_time = dt.now()
    path = save_path + f"DartAPI_{year}년_{report_type}\\"
    all_files = glob(path + f"*_{year}년_{report_type}.pkl")
    df = pd.concat([pd.read_pickle(file) for file in all_files])  # 전체 pickle 합치기
    df.drop(columns=['sj_div', 'reprt_code'], inplace=True)
    #----------------------------------------------------------------------
    # for fs_type in fs_types:  # 재무제표 종류별로 전체 년도 저장
    fs_types = ['재무상태표', '손익계산서', '포괄손익계산서', '현금흐름표']
    for fs_type in fs_types:  # 재무제표 종류별로 필터링 후 저장
        temp = df[df['sj_nm'] == fs_type].copy()
        count_by_cols = temp.describe()[temp.describe().index == 'count'].T  # 컬럼별로 행수 나와있는 df
        use_cols = count_by_cols[count_by_cols['count']> 0].index.tolist()  # 행수가 0인것만 유지
        cols = temp.columns.tolist()  # 전체 df의 컬럼
        for col in cols:
            if col not in use_cols:  # 행수가 0이 아닌 컬럼인지 확인
                temp.drop(columns=col, inplace=True)  # 행수가 0인 컬럼은 삭제
            else:
                pass
        temp.to_pickle(save_path + f"DartAPI_{fs_type}_취합본\{year}_{report_type}_{fs_type}_1차취합본.pkl")
    print('총 소요시간: ' + str(dt.now() - start_time))
#------------------------------------------------------------------------------------------------------------
def change_fs_colname(df): # dart api로 받은거 2차 취합
    '''
    1차 취합본의 컬럼에 있는 날짜부분을 수정하고 재무제표 종류별로 전체 년도 취합. 2차 취합용 생성
    '''
    df.rename(columns={'sj_nm': '재무제표종류'}, inplace=True)
    drop_cols = ['thstrm_nm', 'frmtrm_nm', 'bfefrmtrm_nm', 'fs_div', 'sj_div', 'rcept_no', 'ord', 'frmtrm_q_nm', 'account_detail']  # 필요없는 컬럼 목록
    dimention_cols = []
    for col in df.columns.tolist(): # 컬럼마다 필요없는 컬럼 삭제하고 차원 컬럼은 차원컬럼 list에 추가
        if col in drop_cols:
            df.drop(columns=col, inplace=True)
        elif 'amount' in col:
            pass
        else:
            dimention_cols.append(col)  # 차원 컬럼 별도 리스트에 저장
    df.drop_duplicates(inplace=True)
    df = df.melt(id_vars=dimention_cols, var_name='날짜', value_name='금액')  # 컬럼에 있는 날짜를 행으로 변경
    df.dropna(subset=['금액'], inplace=True) # 금액이 null인거 삭제
    df['금액'] = df['금액'].str.replace(',', '') # 금액에 있는 ,(쉼표) 제거
    df = df[(df['금액'] != '-')] # 금액에 null 대신 '-'로 되어 있는거 제거
    df = df[(df['금액'].apply(len) > 0)] # 금액 컬럼의 길이가 0보다 큰 것만 유지
    value_data = []
    for value in df['금액']:
        if value.startswith('-'):
            value_data.append(float(value.split('-')[1])*(-1))
        else:
            value_data.append(float(value))
    df['금액'] = value_data

    df.loc[~(df['날짜'].str.contains('add')), '누적/개별'] = '개별'  # 분기  thstrm_amount   frmtrm_q_amount
    df.loc[df['날짜'].str.contains('add'), '누적/개별'] = '누적'  # 분기 누적 thstrm_add_amount frmtrm_add_amount
    df.loc[df['재무제표종류'] == '재무상태표', '누적/개별'] = '누적'  # 재무상태표는 항상 누적 bfefrmtrm_amount
    df.loc[df['주기'] == '연간', '누적/개별'] = '누적'  # 연간 재무상태표는 항상 누적
    df.loc[df['날짜'] == 'frmtrm_amount', '누적/개별'] = '누적'  # 전기말은 항상 누적 frmtrm_amount
    df.loc[df['날짜'] == 'bfefrmtrm_amount', '누적/개별'] = '누적'  # 전전기말은 항상 누적 frmtrm_amount
    temp_list = df[['bsns_year', '주기', '날짜']].values.tolist()
    date_values = []
    for value in temp_list:  # 날짜를 재무제표와 재무제표종류(1분기/반기/3분기/연간)에 맞게 변경
        year, term, date_col = value
        this_year = year
        last_year = str(int(year)-1)
        prior_year = str(int(year)-2)
        term_suffix = {'1분기': '0331', '반기': '0630', '3분기': '0930', '연간': '1231'}
        date_suffix = term_suffix[term]
        date_dict = {}
        date_dict.update({'thstrm_amount': this_year + date_suffix})  # 당기
        date_dict.update({'thstrm_add_amount': this_year + date_suffix})  # 당기누적
        date_dict.update({'frmtrm_q_amount': last_year + date_suffix})  # 전기
        date_dict.update({'frmtrm_add_amount': last_year + date_suffix})  # 전기누적
        date_dict.update({'bfefrmtrm_amount': prior_year + '1231'})  # 전전기말
        date_dict.update({'frmtrm_amount': last_year + '1231'})  # 전기말
        date_values.append(date_dict[date_col])
    df.drop(columns=['주기', 'bsns_year'], inplace=True)
    df['날짜'] = date_values
    df = df.drop_duplicates().reset_index(drop=True)
    #--------------------------------------------------------------------------------------------------------
    # 4분기는 누적만 있고 4Q만의 실적이 따로 없음.
    # 그래서 4Q누적에서 3Q누적을 빼서 구해줘야함. 아래 코드가 그 작업하는거
    df['년도'] = df['날짜'].str[:4]
    df['분기'] = df['날짜'].str[4:].map({'1231': '4Q', '0930': '3Q', '0630': '2Q', '0331': '1Q', }) + '_' + df['누적/개별']
    df.drop(columns=['날짜', '누적/개별'], inplace=True)  # 필요없는 컬럼 삭제
    df.drop_duplicates(inplace=True)
    if df['재무제표종류'].str.contains('손익계산서').sum() > 0:  # 포괄손익계산서, 손익계산서는 아래 작업 시행
        df = df.pivot_table(index=['corp_code', '개별/연결', '재무제표종류', 'account_nm', 'account_id', '년도'], columns='분기', values='금액').reset_index()
        df['4Q_개별'] = df['4Q_누적'] - df['3Q_누적']
        df = df.melt(id_vars=['corp_code', '개별/연결', '재무제표종류', 'account_nm', 'account_id', '년도'], var_name='분기', value_name='금액').reset_index(drop=True)
        df = df.dropna(subset=['금액'])
    elif df['재무제표종류'].str.contains('현금흐름표').sum() > 0:  # 현금흐름표는 아래 작업 시행
        df = df.pivot_table(index=['corp_code', '개별/연결', '재무제표종류', 'account_nm', 'account_id', '년도'], columns='분기', values='금액').reset_index()
        df['1Q_누적'] = df['1Q_개별']
        df['2Q_누적'] = df['1Q_개별'] + df['2Q_개별']
        df['3Q_누적'] = df['3Q_개별'] + df['2Q_누적']
        df['4Q_개별'] = df['4Q_누적'] - df['3Q_누적']
        df = df.melt(id_vars=['corp_code', '개별/연결', '재무제표종류', 'account_nm', 'account_id', '년도'], var_name='분기', value_name='금액').reset_index(drop=True)
        df = df.dropna(subset=['금액'])
    else: # 재무상태표는 아래 작업 시행
        pass
    df[['분기', '개별/누적']] = df['분기'].str.split('_', expand=True)
    df['날짜'] = df['년도'] + df['분기'].map({'4Q': '1231', '3Q': '0930', '2Q': '0630', '1Q': '0331'})
    df.drop(columns=['년도', '분기'], inplace=True)
    df.dropna(subset=['금액'], inplace=True)
    df['account_id'] = df['account_id'].str.split('_', expand=True)[1] # account_id앞에 ifrs, dart로 시작되어서 삭제
    df['account_nm'] = df['account_nm'].str.strip() # 한글 계정명이 공란으로 시작되는 경우가 있어서 없애줌
    df = df.drop_duplicates().reset_index(drop=True)
    return df
#------------------------------------------------------------------------------------------------------------
def union_n_change_cols(): # dart api로 받은거 3차 취합
    '''
    기간별로 나뉘져 있는 df를 하나로 합치고 컬럼명 통일
    '''
    start_time_total = dt.now()
    fs_types = ['재무상태표', '손익계산서', '포괄손익계산서', '현금흐름표']
    for fs_type in fs_types:
        start_time = dt.now()
        all_files = glob(save_path + f"DartAPI_{fs_type}_취합본\*_1차취합본.pkl")
        all_files.sort(reverse=True)
        df = pd.concat([pd.read_pickle(file) for file in all_files])
        df = change_fs_colname(df)
        # df = pd.concat([change_fs_colname(pd.read_pickle(file)) for file in all_files])
        df.to_pickle(save_path + f'DartAPI_{fs_type}_취합본/전체_{fs_type}_2차취합본.pkl')
        hyper_path = save_path + f'DartAPI_{fs_type}_취합본/전체_{fs_type}_2차취합본.hyper'
        pantab.frame_to_hyper(df, hyper_path, table=f'DartAPI_{fs_type}')
        print(f'{fs_type} 완료: ' + str(dt.now()-start_time))
    print('총 소요시간: ' + str(dt.now() - start_time_total))
    #------------------------------------------------------------------------------
    # 재무제표 계정 전처리 작업을 위해서 취합된 df에 있는 고유계정만 google spreadsheet DartApi 시트에 업로드
    df_all = pd.DataFrame()
    for fs_type in fs_types:
        df = pd.read_pickle(save_path + f"DartAPI_{fs_type}_취합본\*_2차취합본.pkl")
        df = df[['재무제표종류','account_id','account_nm']].drop_duplicates().copy()
        df_all = df_all.append(df)
    df_all = df_all.drop_duplicates().sort_values(by=['재무제표종류','account_nm','account_id']).reset_index(drop=True)
    gfile = gc.open('재무제표 index')
    gfile.worksheet_by_title('DartApi').set_dataframe(df_all, 'A1', fit=True)
    print('재무제표 고유계정 Google SpreadSheet 업로드 완료')
    #------------------------------------------------------------------------------
    # 재무제표 계정 순서랑 추후의 재무제표 url 생성을 위해 취합된 df에서 필요한 컬럼만 추리기
    df_all = pd.DataFrame()
    for fs_type in fs_types:
        all_files = glob(save_path + f"DartAPI_{fs_type}_취합본\*_1차취합본.pkl")
        df = pd.concat([pd.read_pickle(all_files) for file in all_files])
        # df = df[['재무제표종류', 'corp_code', 'rcept_no', 'account_id','account_nm', 'ord']].drop_duplicates().copy()
        df_all = df_all.append(df)
    df_all = df_all.drop_duplicates().sort_values(
                            by=['corp_code', 'rcept_no','재무제표종류','account_nm','account_id','ord']).reset_index(drop=True)
    df['account_id'] = df['account_id'].str.split('_', expand=True)[1]
    # gfile = gc.open('재무제표 index')
    # gfile.worksheet_by_title('DartApi').set_dataframe(df_all, 'A1', fit=True)
#--------------------------------------------------------------------------------------------------------
def opendart_reader_get_finstate_date(year, report_type):
    '''
    년도와 1분기/반기/3분기/연간 을 넣으면 해당되는 시점의 주요 재무제표 현황 가져옴
    OpenDartReader 사용
    '''
    start_time_total = dt.now()
    report_dict = {'1분기': 11013, '반기': 11012, '3분기': 11014, '연간': 11011}
    df = pd.DataFrame()
    for value in code_list['회사코드']:
        try:
            temp_df = dart.finstate(value, year, report_dict[report_type])
            df = df.append(temp_df)
            time.sleep(1.5)
        except:
            print(value)
            time.sleep(1)
    df.to_pickle(finstate_path+f'{year}년_{report_type}_주요재무제표지표.pkl')
    print(f'{year}년_{report_type}_주요재무제표지표 저장 완료')
    print(str(len(df))+ '행, 총 소요시간: ' + str(dt.now() - start_time_total))
#--------------------------------------------------------------------------------------------------------
def opendart_reader_union_finstates(): # OpenDartReader 사용해서 받은 재무제표 주요값 있는 pickle 합쳐서 hyper로 추출
    '''
    OpenDartReader 사용해서 받은 재무제표 주요값 있는 pickle 합쳐서 hyper로 추출
    '''
    paths = glob(finstate_path +'*_*.pkl')
    paths.sort(reverse=True)
    df = pd.concat([pd.read_pickle(path) for path in paths])
    df['fs_nm'] = df['fs_nm'].map({'연결재무제표': '연결', '재무제표': '개별'})
    df.rename(columns={'fs_nm': '연결/개별', 'sj_nm': '재무제표종류'}, inplace=True)
    drop_cols = ['thstrm_nm', 'frmtrm_nm', 'bfefrmtrm_nm', 'fs_div', 'sj_div', 'rcept_no', 'ord']  # 필요없는 컬럼 목록
    dimention_cols = []
    for col in df.columns.tolist():
        if 'dt' in col or col in drop_cols:  # 필요없는 컬럼 삭제
            df.drop(columns=col, inplace=True)
        elif 'amount' in col:
            pass
        else:
            dimention_cols.append(col)  # 차원 컬럼 별도 리스트에 저장
    df.drop_duplicates(inplace=True)
    df = df.melt(id_vars=dimention_cols, var_name='날짜', value_name='금액')  # 컬럼에 있는 날짜를 행으로 변경
    df.dropna(subset=['금액'], inplace=True)
    df['금액'] = df['금액'].str.replace(',', '')
    df = df[(df['금액'] != '-')]
    df = df[(df['금액'].apply(len) > 0)]
    value_data = []
    for value in df['금액']:
        if value.startswith('-'):
            value_data.append(int(value.split('-')[1])*(-1))
        else:
            value_data.append(int(value))
    df['금액'] = value_data
    df.loc[~(df['날짜'].str.contains('add')), '누적/개별'] = '개별'  # 분기
    df.loc[df['날짜'].str.contains('add'), '누적/개별'] = '누적'  # 분기 누적
    df.loc[df['재무제표종류'] == '재무상태표', '누적/개별'] = '누적'  # 재무상태표는 항상 누적
    df.loc[df['reprt_code'] == '11011', '누적/개별'] = '누적'  # 연간 재무상태표는 항상 누적

    temp_list = df[['bsns_year', 'reprt_code', '날짜', '재무제표종류']].values.tolist()
    date_values = []
    for value in temp_list:  # 날짜를 재무제표와 재무제표종류(1분기/반기/3분기/연간)에 맞게 변경
        year, term, date_col, tp = value
        this_year = year
        last_year = str(int(year)-1)
        prior_year = str(int(year)-2)
        report_dict = {'11013': '1분기',
                    '11012': '반기',
                    '11014': '3분기',
                    '11011': '연간'}
        term = report_dict[term]
        term_suffix = {'1분기': '0331',
                    '반기': '0630',
                    '3분기': '0930',
                    '연간': '1231'}
        date_suffix = term_suffix[term]
        date_dict = {}
        date_dict.update({'thstrm_amount': this_year + date_suffix})  # 당기
        date_dict.update({'thstrm_add_amount': this_year + date_suffix})  # 당기누적
        date_dict.update({'frmtrm_add_amount': last_year + date_suffix})  # 전기누적
        date_dict.update({'bfefrmtrm_amount': prior_year + '1231'})  # 전전기
        if tp == '재무상태표':
            date_dict.update({'frmtrm_amount': last_year + '1231'})  # 전기말
        else:
            date_dict.update({'frmtrm_amount': last_year + date_suffix})  # 전기
        date_values.append(date_dict[date_col])
    df['날짜'] = date_values
    df.drop(columns=['reprt_code', 'bsns_year'], inplace=True)
    # 'rcept_no'때문에 'rcept_no'를 삭제하면 행이 있기 때문에 중복제거를 해줘야함
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)
    #--------------------------------------------------------------------------------------------------------
    # 4분기는 누적만 있고 4Q만의 실적이 따로 없음.
    # 그래서 4Q누적에서 3Q누적을 빼서 구해줘야함. 아래 코드가 그 작업하는거
    df['년도'] = df['날짜'].str[:4]
    df['분기'] = df['날짜'].str[4:].map({'1231': '4Q',
                                    '0930': '3Q',
                                    '0630': '2Q',
                                    '0331': '1Q', }) + '_' + df['누적/개별']
    df.drop(columns=['날짜', '누적/개별'], inplace=True)  # 필요없는 컬럼 삭제

    df.drop_duplicates(inplace=True)
    loop_list = ['재무상태표', '손익계산서']
    for fstype in loop_list:
        if fstype == '손익계산서':
            temp = df[df['재무제표종류'] == fstype].copy()  # 손익계산서만 필터링
            temp = temp.pivot_table(index=['corp_code', 'stock_code', '연결/개별',
                                        '재무제표종류', 'account_nm', '년도'], columns='분기', values='금액').reset_index()
            temp['4Q_개별'] = temp['4Q_누적'] - temp['3Q_누적']
            temp = temp.melt(id_vars=['corp_code', 'stock_code', '연결/개별', '재무제표종류',
                                    'account_nm', '년도'], var_name='분기', value_name='금액').reset_index(drop=True)
            temp = temp.dropna(subset=['금액'])
        else:
            temp2 = df[df['재무제표종류'] == fstype].copy() # 재무상태표 필터링
    df = pd.concat([temp, temp2], axis=0)
    df[['분기', '개별/누적']] = df['분기'].str.split('_', expand=True)
    df['날짜'] = df['년도'] + df['분기'].map({'4Q': '1231',
                                        '3Q': '0930',
                                        '2Q': '0630',
                                        '1Q': '0331'})
    df.drop(columns=['년도', '분기'], inplace=True)
    df.drop_duplicates(inplace=True)
    df.dropna(subset=['금액'], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.to_pickle(finstate_path+'주요재무제표지표취합본.pkl')
    if platform.system() == 'Windows':
        path = r"C:\Users\bong2\OneDrive\DataArchive\DB_주식관련\hyper files\\"
    else:
        path = r'/Users/jbl_mac/OneDrive/DataArchive/DB_주식관련/hyper files//'
    pantab.frame_to_hyper(df, path + '주요재무제표지표_취합.hyper', table='data')
#--------------------------------------------------------------------------------------------------------
def get_notice_by_date(date):
    '''
    날짜를 yyyy-mm-dd 타입으로 넣으면 해당되는 날짜의 전체 공시 리스트를 가져옴
    '''
    # df = dart.list_date_ex(date)
    # df.to_pickle(finstate_path+f'{date}_전체공시리스트.pkl')

    url = 'https://opendart.fss.or.kr/api/list.json'
    begin_date = '20200426'
    params = {'crtfc_key': api_key,
                'pblntf_ty': begin_date,
            'bgn_de': begin_date,
            #   'page_no': 2,
            'page_count': 100}
    r = requests.get(url, params=params)
#------------------------------------------------------------------------------------------------------------
def update_fs_taxonomy():  # 사용안함 (DART API로 받아오면 depth가 나오지 않음)
    gfile = gc.open('재무제표 index')
    url = 'https://opendart.fss.or.kr/api/xbrlTaxonomy.json'
    fs_types = ['BS1',
                'BS2',
                'BS3',
                'BS4',
                'IS1',
                'IS2',
                'IS3',
                'IS4',
                'CIS1',
                'CIS2',
                'CIS3',
                'CIS4',
                'DCIS1',
                'DCIS2',
                'DCIS3',
                'DCIS4',
                'DCIS5',
                'DCIS6',
                'DCIS7',
                'DCIS8',
                'CF1',
                'CF2',
                'CF3',
                'CF4',
                'SCE1',
                'SCE2']
    for ts_type in fs_types:
        params = {'crtfc_key': api_key,
                  'sj_div': ts_type}
        r = requests.get(url, params=params)
        df = pd.DataFrame(r.json()['list'])
        gfile.worksheet_by_title(ts_type).clear("*")
        gfile.worksheet_by_title(ts_type).set_dataframe(df, 'A1', fit=True)
#------------------------------------------------------------------------------------------------------------
def update_fs_taxonomy_from_file(): # 엑셀 재무제표 택사노미를 시트별로 Google spreadsheet에 업로드
    '''
    엑셀 재무제표 택사노미를 시트별로 Google spreadsheet에 업로드
    '''
    if platform.system() == 'Windows':
        taxonomy = r"C:\Users\bong2\OneDrive\DataArchive\DB_주식관련\DB_주식분석_재무제표\재무제표_택사노미_20200622.xlsx"
    else:
        taxonomy = r'/Users/jbl_mac/OneDrive/DataArchive/DB_주식관련/DB_주식분석_재무제표/재무제표_택사노미_20200622.xlsx'
    df_all = pd.ExcelFile(taxonomy)
    # 자본변동표에 해당하는 SCE1,SCE2는 작업 안함
    valid_sheets = ['BS1',
                    'BS2',
                    'BS3',
                    'BS4',
                    'IS1',
                    'IS2',
                    'IS3',
                    'IS4',
                    'CIS1',
                    'CIS2',
                    'CIS3',
                    'CIS4',
                    'DCIS1',
                    'DCIS2',
                    'DCIS3',
                    'DCIS4',
                    'DCIS5',
                    'DCIS6',
                    'DCIS7',
                    'DCIS8',
                    'CF1',
                    'CF2',
                    'CF3',
                    'CF4']
    for sheet in valid_sheets:
        df = df_all.parse(sheet_name=sheet)
        df.drop(columns=['IFRS Reference', 'Element ID', '영문 Label'], inplace=True)
        filt1 = df['Depth'].notna()
        filt2 = df['Depth'] > 0
        df = df[filt1 & filt2]
        df.drop_duplicates(inplace=True)
        df.reset_index(drop=True, inplace=True)
        df = df.reset_index().rename(columns={'index': '순번'})
        # 한굴 Label 컬럼에 있는 '[abstract\]' 문자열 '총계'로 변경
        df['한글 Label'] = df['한글 Label'].str.replace(' \[abstract\]', '총계').str.strip()
        df = df[['순번', '한글 Label', 'Element Name', 'Depth', 'Data Type']]
        # 한굴 Label 컬럼에 '총계'가 있으면 'Depth'를 1로 변경
        try:
            condition = df['한글 Label'].str.contains('총계')
            df.loc[condition, 'Depth'] = 1.0
        except:
            pass
        gfile = gc.open('재무제표 index')
        gfile.worksheet_by_title(sheet).set_dataframe(df, 'A1', fit=False)
#------------------------------------------------------------------------------------------------------------
def get_fs_taxonomy_from_gsheet(): # google spreadsheet에 있는 시트를 전체 합쳐서 하나의 재무제표 게정 텍사노미 df를 반환
    '''
    google spreadsheet에 있는 시트를 전체 합쳐서 하나의 재무제표 게정 텍사노미 df를 반환
    '''
    url = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vQV2D1HkTRXJXD3mrX0IAxZT_RjdX5UYF7g3WzqUPRx74vd5NYIiflz_Du2Cc5m9woT8t-BApfXTtle/pub?output=xlsx'
    df = pd.ExcelFile(url)
    fs_types = df.parse(sheet_name='재무제표종류')['재무제표구분'].values.tolist()
    df_all = pd.DataFrame()
    for fs_type in fs_types:  # 시트별로 돌면서 df가져오기
        temp_df = df.parse(sheet_name=fs_type)
        temp_df.insert(0, 'fs_type', fs_type)
        temp_df.drop_duplicates(inplace=True)
        df_all = df_all.append(temp_df)
    del df, fs_types
    df_all.drop(columns=['검증', 'Depth'], inplace=True)  # 필요없는 컬럼 삭제
    filt = df_all['fs_type'].str.contains('SCE') # 자본변동표는 삭제
    df_all = df_all[~filt].copy()
    df_all.drop_duplicates(subset='Element Name', inplace=True)
    df_all.rename(columns={1: 'Level1',
                           2: 'Level2',
                           3: 'Level3',
                           4: 'Level4',
                           5: 'Level5'}, inplace=True)
    fs_types = []
    for fs_type in df_all['fs_type'].values.tolist():
        if 'CIS' in fs_type:
            fs_types.append('포괄손익계산서')
        elif 'IS' in fs_type:
            fs_types.append('손익계산서')
        elif 'CF' in fs_type:
            fs_types.append('현금흐름표')
        else:
            fs_types.append('재무상태표')
    df_all['재무제표종류'] = fs_types # 재무제표종류 컬럼 추가
    df_all.drop(columns='fs_type', inplace=True) # 영문코드로 되어 있던 컬럼은 삭제
    return df_all.drop_duplicates().reset_index(drop=True)
#------------------------------------------------------------------------------------------------------------
def change_codes(value): # 항목코드 수정 함수
        if value.split('_')[0] == ('ifrs' or 'dart') :
            return value.split('_')[1]
        else:
            return value.split('_')[-1]
def change_values(value): # 금액 수정 함수
        if value.startswith('-'): # 음수가 '-' 기호로 시작하는 경우
            return float(value.split('-')[1]) * (-1)
        elif value.startswith('('): # 음수가 괄호안에 들어 있는 경우
            return float(value.split('(')[1][:-1]) * (-1)
        else: # 음수가 '-'로 시작하거나 괄호안에 있는 경우가 아니면 양수로 판단
            return float(value)
#------------------------------------------------------------------------------------------------------------
def clean_dart_fs_file(filepath): # dart에서 다운받은 재무제표 txt 파일 주소를 넣으면 전처리 된 dataframe를 반환하는 함수
    '''
    dart에서 다운받은 재무제표 txt 파일 주소를 넣으면 전처리 된 dataframe를 반환하는 함수
    '''
    start_time = dt.now()
    colist = common.from_gsheet('DB_기업정보', 'FS_update_list')['종목코드']
    df = pd.read_csv(filepath, sep='\t', encoding='cp949', dtype='str')
    # 컬럼정리
    df['연결/별도'] = df['재무제표종류'].str.split('-', expand=True)[1] # 연결/별도 기준 컬럼 생성
    df['재무제표종류'] = df['재무제표종류'].str.split('-', expand=True)[0] # 재무제표종류
    df['종목코드'] = df['종목코드'].str[1:7] # 종목코드에 기호부분 삭제
    df['시장구분'] = df['시장구분'].map({'유가증권시장상장법인':'코스피','코스닥시장상장법인':'코스닥'}) # 시장명칭 수정
    # 상장폐지된 종목 필터링
    del_code_list = list(set(df['종목코드']) - set(colist))
    filt = df['종목코드'].isin(del_code_list)
    df = df.loc[~filt,:]
    # 날짜 컬럼 명칭 수정을 위해 필요한 값 생성
    yyyy = df['결산기준일'].str.split('-', expand=True)[0].unique()[0]
    yyyy_last = str(int(yyyy)-1)
    yyyy_last2 = str(int(yyyy)-2)
    quarter = df['보고서종류'].unique().tolist()[0] # 1분기보고서 / 반기보고서 / 3분기보고서 / 사업보고서
    # 필요없는 컬럼 삭제
    count_by_cols = df.describe()[df.describe().index == 'count'].T # null인 컬럼 삭제 기준1 _ 컬럼명 value 갯수
    del_cols = count_by_cols[count_by_cols['count'] < 1].index.tolist() # null인 컬럼 삭제 기준2 _ 컬럼별 value 갯수가 0이하인것 필터
    [df.drop(columns=col, inplace=True) for col in del_cols] # null인 컬럼 삭제
    df.drop(columns=['업종', '보고서종류'], inplace=True) # 추가로 필요없는 컬럼 삭제
    # 컬럼 그룹 생성 및 컬럼 수정위해 필요한 변수 생성
    dimention_cols = ['재무제표종류','종목코드','회사명','시장구분','업종명','결산월','결산기준일','연결/별도','통화','항목코드','항목명']
    # 차원 컬럼의 값들 앞/뒤 공백 제거
    for col in dimention_cols:
        df[col] = df[col].str.strip()
    value_cols = set(df.columns.tolist()) - set(dimention_cols)
    report_type_dict = {'1분기보고서' : '1Q', '반기보고서' : '2Q', '3분기보고서' : '3Q', '사업보고서': '4Q'}
    quarter = report_type_dict[quarter]
    value_cols_dict = {# 1Q IS
                        '당기 1분기 3개월' : yyyy + '_' + quarter + '_분기',
                        '전기 1분기 3개월' : yyyy_last + '_' + quarter + '_분기',
                        '당기 1분기 누적' : yyyy + '_' + quarter +'_누적',
                        '전기 1분기 누적': yyyy_last + '_' + quarter +'_누적',
                        # 1Q CF
                        '당기 1분기' : yyyy + '_' + quarter +'_누적',
                        '전기 1분기': yyyy_last + '_' + quarter +'_누적',
                        # 1Q BS
                        '당기 1분기말' : yyyy + '_' + quarter +'_누적',
                        '전기말' : yyyy_last + '_4Q_누적',
                        '전전기말' : yyyy_last2 + '_4Q_누적',
                        # 2Q IS
                        '당기 반기 3개월' : yyyy + '_' + quarter + '_분기',
                        '전기 반기 3개월' : yyyy_last + '_' + quarter + '_분기',
                        '당기 반기 누적' : yyyy + '_' + quarter +'_누적',
                        '전기 반기 누적' : yyyy_last + '_' + quarter +'_누적',
                        # 2Q BS
                        '당기 반기말' : yyyy + '_' + quarter + '_누적',
                        '전기말' : yyyy_last + '_4Q_누적',
                        '전전기말' : yyyy_last2 + '_4Q_누적',
                        # 2Q CF
                        '당기 반기' : yyyy + '_' + quarter +'_누적',
                        '전기 반기' : yyyy_last + '_' + quarter +'_누적',
                        # 3Q CF
                        '당기 3분기' : yyyy + '_' + quarter +'_누적',
                        '전기 3분기' : yyyy_last + '_' + quarter +'_누적',
                        # 3Q BS
                        '당기 3분기말' : yyyy + '_' + quarter + '_누적',
                        '전기말' : yyyy_last + '_4Q_누적',
                        '전전기말' : yyyy_last2 + '_4Q_누적',
                        # 3Q IS
                        '당기 3분기 3개월' : yyyy + '_' + quarter + '_분기',
                        '전기 3분기 3개월' : yyyy_last + '_' + quarter + '_분기',
                        '당기 3분기 누적' : yyyy + '_' + quarter +'_누적',
                        '전기 3분기 누적' : yyyy_last + '_' + quarter+'_누적',
                        # 4Q 공통
                        '당기' : yyyy + '_4Q_누적',
                        '전기' : yyyy_last + '_4Q_누적',
                        '전전기' : yyyy_last2 + '_4Q_누적'}
    # 날짜 컬럼 이름 변경
    [df.rename(columns={col:value_cols_dict[col]}, inplace=True) for col in value_cols]
    dimention_cols.append('계정순서')
    df_all = pd.DataFrame()
    for code in df['종목코드'].unique().tolist():
        # 1개 회사만 선택
        temp_df = df[df['종목코드']==code]
        # 항목코드 수정 함수 적용
        temp_df['항목코드'] = temp_df['항목코드'].apply(change_codes)
        # 계정순서 컬럼 생성
        temp_df = temp_df.reset_index(drop=True).reset_index().rename(columns={'index':'계정순서'})
        # dataframe wide to tidy
        temp_df = temp_df.melt(id_vars=dimention_cols, var_name='날짜', value_name='금액').drop_duplicates()
        # 금액커럼에 값이 없거나 이상한 게 들어가 있으면 삭제
        temp_df = temp_df[temp_df['금액'].notna()]
        temp_df['금액'] = temp_df['금액'].str.replace(',', '') # 문자열인 금액 컬럼을 숫자로 수정
        temp_df = temp_df[(temp_df['금액'] != '-')]
        temp_df = temp_df[(temp_df['금액'].apply(len) > 0)]
        temp_df['금액'] = temp_df['금액'].apply(change_values)
        # 하나의 회사에서도 항목명이 같은데 depth 구분하느라 똑같이 들어가 있는게 있어서 중복제거 해줘야함
        temp_df.drop_duplicates(subset=['항목명', '날짜', '금액'], inplace=True)
        # null값 제외하고 다시 tidy to wide dataframe
        temp_df = temp_df.pivot_table(index=dimention_cols,columns='날짜', values='금액').reset_index()
        temp_df = temp_df.sort_values(by='계정순서').reset_index(drop=True) # 계정순서순으로 정렬
        temp_df['계정순서'] = temp_df.index.tolist() # 계정순서 다시 셋팅
        temp_df = temp_df.melt(id_vars=dimention_cols, var_name='날짜', value_name='금액')
        temp_df = temp_df.dropna().drop_duplicates()
        df_all = df_all.append(temp_df)
    filename = filepath.split('\\')[-1][:-13]
    if '손익계산' in filename:
        fstype = '손익계산서'
    elif '재무상태' in filename:
        fstype = '재무상태표'
    else:
        fstype = '현금흐름표'
    if platform.system() == 'Windows':
        folder = r"C:\Users\bong2\OneDrive\DataArchive\DB_주식관련\DB_주식분석_재무제표\FS_raw_file_from_DART\2_{}_1차전처리\\".format(fstype)
    else:
        folder = r'/Users/jbl_mac/OneDrive/DataArchive/DB_주식관련/DB_주식분석_재무제표/FS_raw_file_from_DART/2_{}_1차전처리//'.format(fstype)
    df_all.to_pickle(folder+filename+'.pkl')
    print(f'{filename} 완료, 소요시간: ' + str(dt.now() - start_time))
    return df_all.reset_index(drop=True)
#------------------------------------------------------------------------------------------------------------
def union_dart_fs_files():
    start_time_total = dt.now()
    fs_types = ['재무상태표', '손익계산서', '현금흐름표']
    if platform.system() == 'Windows':
        fs_file_folder = r"C:\Users\bong2\OneDrive\DataArchive\DB_주식관련\DB_주식분석_재무제표\FS_raw_file_from_DART\\"
    else:
        fs_file_folder = r'/Users/jbl_mac/OneDrive/DataArchive/DB_주식관련/DB_주식분석_재무제표/FS_raw_file_from_DART//'
    for fstype in fs_types:
        start_time = dt.now()
        paths = glob(fs_file_folder + f"1_{fstype}_다운받은원본\*.txt")
        paths.sort(reverse=True)
        df = pd.concat([clean_dart_fs_file(path) for path in paths])
        df = df.drop_duplicates().reset_index(drop=True)
        df.to_pickle(fs_file_folder + f'전체_{fstype}_1차취합본.pkl')
        # df = df[['항목코드', '항목명', '재무제표종류','연결/별도','계정순서']].drop_duplicates().sort_values(by=[
        #     '계정순서','항목명', '항목코드','재무제표종류','연결/별도'])
        # common.to_hyper(df, fs_file_folder+f'{fstype}')
        print(f'{fstype} 완료. 소요시간: ' + str(dt.now()-start_time))
    print('전체 완료. 총소요시간: ' + str(dt.now() - start_time_total))
