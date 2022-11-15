from pathlib import Path
from re import S
import pandas as pd
import numpy as np
import datetime
from dateutil import relativedelta
import re


def Create_DF(file_path):
    print('Creating data frame of file ', file_path, '...')
    df = pd.read_excel(file_path)
    # print(df['Bearbetning: Start'])
    return df


def Create_DF_from_CSV(file_path):
    print('Creating data frame of file ', file_path, '...')
    df = pd.read_csv(file_path)
    # print(df['Bearbetning: Start'])
    return df


def Create_DFs_List(file_path, sheet_list):
    dict_df = pd.read_excel(file_path, sheet_name=sheet_list)
    return dict_df


def Conact_DFs(path_of_ref_dfs):
    frames = []

    for i in range(0, len(path_of_ref_dfs)):
        temp_df = Create_DF(path_of_ref_dfs[i])
        frames.append(temp_df)

    result_df = pd.concat(frames)
    print(result_df)
    return result_df


def Save_Excel_File(df, filePath, ext, folderName, abs_path):
    file_name = Path(filePath).stem + ext
    file_path_to_be_saved_at = str(abs_path) + folderName + file_name

    path = Path(file_path_to_be_saved_at)
    path.parent.mkdir(parents=True, exist_ok=True)

    df.to_excel(file_path_to_be_saved_at, index=0)
    print(file_name, " saved at ", str(abs_path) + folderName)
    return file_path_to_be_saved_at


def Conact_DFs(df1, df2):

    frames = [df1, df2]
    result_df = pd.concat(frames)
    # print(result_df)
    return result_df


def Compare_Dates(d1, d2):

    if d1 <= d2:
        return d1

    elif d2 <= d1:
        return d2


def Clean_IPR_Df_Data(IPR_df: pd.DataFrame, H2020_df: pd.DataFrame, IPR_file_path, abs_path):

    print("Cleaning IPR data....")

    H2020_df = H2020_df.sort_values('Project ID')
    H2020_removed_dups_df = H2020_df.drop_duplicates('Project ID')

    H2020_removed_dups_df['Project Start Date'] = pd.to_datetime(
        H2020_removed_dups_df['Project Start Date'], format="%d/%m/%Y", errors='coerce')

    ##########################################################################################################################################################

    IPR_df = IPR_df.sort_values('project ID')

    # IPR_df['application date'] = IPR_df['application date'].fillna(pd.Timestamp('2022-12-25'))
    IPR_df['application date'] = pd.to_datetime(
        IPR_df['application date'], format="%Y-%m-%d", errors='coerce')

    # IPR_df['priority date'] = IPR_df['priority date'].fillna(pd.Timestamp('2022-12-25'))
    IPR_df['priority date'] = pd.to_datetime(
        IPR_df['priority date'], format="%Y-%m-%d", errors='coerce')

    for IPR_index, IPR_row in IPR_df.iterrows():

        project_id = IPR_row['project ID']
        print("For ", project_id)

        if not pd.isnull(project_id):

            IPR_type = IPR_row['IPR']

            application_date = IPR_row['application date']

            priority_date = IPR_row['priority date']

            if (not pd.isnull(application_date)) or (not pd.isnull(priority_date)):

                H2020_df_Filtered = H2020_removed_dups_df.loc[
                    H2020_removed_dups_df['Project ID'] == project_id]
                print("Finding in H2020 table...")

                if not H2020_df_Filtered.empty:
                    print('Found!!!')

                    start_date = ''
                    for i, row in H2020_df_Filtered.iterrows():

                        start_date = row['Project Start Date']

                        print(start_date, " is start date")
                        print(application_date, " is application date")
                        print(priority_date, " is priority date")

                        if not pd.isnull(start_date):

                            if (not pd.isnull(application_date)) and (not pd.isnull(priority_date)):
                                print('Finding the smaller date...')

                                smaller_date = Compare_Dates(
                                    application_date, priority_date)
                                print("Found!!!")
                                print(smaller_date, " is smaller date")

                                r = relativedelta.relativedelta(
                                    smaller_date, start_date)

                                months_difference = (r.years * 12) + r.months
                                print(months_difference,
                                      " is month difference")

                                if (months_difference > 12) and (IPR_type == "BACKGROUND"):
                                    print(
                                        "For ", IPR_type, " months difference b/w start and application/priority is ", months_difference)
                                    IPR_df.at[IPR_index, 'IPR'] = "FOREGROUND"
                                    print("Changed to ",
                                          IPR_df.at[IPR_index, 'IPR'], "!!!")

                                elif (months_difference < 12) and (IPR_type == "FOREGROUND"):
                                    print(
                                        "For ", IPR_type, " months difference b/w start and application/priority is ", months_difference)
                                    IPR_df.at[IPR_index, 'IPR'] = "BACKGROUND"
                                    print("Changed to ",
                                          IPR_df.at[IPR_index, 'IPR'], "!!!")

                            elif (not pd.isnull(application_date)) and (pd.isnull(priority_date)):
                                print('Finding the smaller date...')

                                print("Found!!!")
                                print(application_date, " is smaller date")

                                r = relativedelta.relativedelta(
                                    application_date, start_date)

                                months_difference = (r.years * 12) + r.months
                                print(months_difference,
                                      " is month difference")

                                if (months_difference > 12) and (IPR_type == "BACKGROUND"):
                                    print(
                                        "For ", IPR_type, " months difference b/w start and application/priority is ", months_difference)
                                    IPR_df.at[IPR_index, 'IPR'] = "FOREGROUND"
                                    print("Changed to ",
                                          IPR_df.at[IPR_index, 'IPR'], "!!!")

                                elif (months_difference < 12) and (IPR_type == "FOREGROUND"):
                                    print(
                                        "For ", IPR_type, " months difference b/w start and application/priority is ", months_difference)
                                    IPR_df.at[IPR_index, 'IPR'] = "BACKGROUND"
                                    print("Changed to ",
                                          IPR_df.at[IPR_index, 'IPR'], "!!!")

                            elif (pd.isnull(application_date)) and (not pd.isnull(priority_date)):
                                print('Finding the smaller date...')

                                print("Found!!!")
                                print(priority_date, " is smaller date")

                                r = relativedelta.relativedelta(
                                    priority_date, start_date)

                                months_difference = (r.years * 12) + r.months
                                print(months_difference,
                                      " is month difference")

                                if (months_difference > 12) and (IPR_type == "BACKGROUND"):
                                    print(
                                        "For ", IPR_type, " months difference b/w start and application/priority is ", months_difference)
                                    IPR_df.at[IPR_index, 'IPR'] = "FOREGROUND"
                                    print("Changed to ",
                                          IPR_df.at[IPR_index, 'IPR'], "!!!")

                                elif (months_difference < 12) and (IPR_type == "FOREGROUND"):
                                    print(
                                        "For ", IPR_type, " months difference b/w start and application/priority is ", months_difference)
                                    IPR_df.at[IPR_index, 'IPR'] = "BACKGROUND"
                                    print("Changed to ",
                                          IPR_df.at[IPR_index, 'IPR'], "!!!")

                            else:
                                continue

    IPR_df['application date'] = IPR_df['application date'].astype(str)
    IPR_df['priority date'] = IPR_df['priority date'].astype(str)

    saved_path = Save_Excel_File(
        IPR_df, IPR_file_path, '(modified).xlsx', '/output/', abs_path)
    print('file saved at ',  saved_path)

    return IPR_df


def Create_List_Of_Unique_Values(df: pd.DataFrame):

    myList = df.unique().tolist()
    return myList


def Cals_For_Codes(H2020_df: pd.DataFrame, IPR_df: pd.DataFrame, code, output_dict, year, Patent_df, Brico_df, Network_total_df, Patents_Social_df):

    # Round 1

    #     ### 1 - Looking for code in column K 'NUTS 3 Code'

    H2020_df_DED51_pub = H2020_df.loc[H2020_df['NUTS 3 Code'] == code]

#     ### 2 - Filtering only Public body from column H 'Legal Entity Type'

    H2020_df_DED51_pub = H2020_df_DED51_pub.loc[H2020_df_DED51_pub["Legal Entity Type"] == "PUB"]

#     ######### FOR COLUMN K for Total number of  public bodies participations, running for at least one year

#         output_dict["Total number of  public bodies participations, running for at least one year"] += For_Col_K(year, H2020_df_DED51_pub)

#     ######### FOR COLUMN L for Total number of unique projects involving public bodies, running for at least one year

#         output_dict["Total number of unique  projects involving public bodies, running for at least one year"] +=  For_Col_L(year, H2020_df_DED51_pub)

#     ######### FOR COLUMN M for Number project months acummulating  since 2014

#         output_dict["Number project months acummulating  since 2014"] +=  For_Col_M(year, H2020_df_DED51_pub)

#     ######### FOR COLUMN N for Multiannual EU funds commitment benefitting public bodies aggregated at regional level (date of contract signature  counts)

#         H2020_df_DED51_pub['Contract signature date'] = pd.to_datetime(H2020_df_DED51_pub['Contract signature date'], format="%d/%m/%Y", errors='coerce')

#         output_dict["Multiannual EU funds commitment benefitting   public bodies aggregated at regional level (date of contract signature  counts)"] +=  For_Col_N(year, H2020_df_DED51_pub)

#     ######### FOR COLUMN O for Multiannual self financing  commitment benefitting   public bodies aggregated at regional level (date of contract signature  counts)

#         output_dict["Multiannual self financing  commitment benefitting   public bodies aggregated at regional level (date of contract signature  counts)"] +=  For_Col_O(year, H2020_df_DED51_pub)

#     ######### FOR COLUMN P Total number of background IPR from another region accessed via a project that has been running 1 year (counted once per project lifetime)

#         output_dict["Total number of background IPR from another region accessed via a project that has been running 1 year (counted once per project lifetime)"] +=  For_Col_P_and_Q(year, IPR_df, 'BACKGROUND', H2020_df, H2020_df_DED51_pub, code)

#     ######### FOR COLUMN Q Total number of foreground IPR from another region accessed via a project that has been running 1 year (counted once per project lifetime)

#         output_dict["Total number of foreground IPR from another region accessed via a project that has been running 1 year (counted once per project lifetime)"] +=  For_Col_P_and_Q(year, IPR_df, 'FOREGROUND', H2020_df, H2020_df_DED51_pub, code)


# # Round 2

#     ### 1 - Looking for code in column K 'NUTS 3 Code'

    H2020_df_DED51_prc = H2020_df.loc[H2020_df['NUTS 3 Code'] == code]

#     ### 2 - Filtering only Public body from column H 'Legal Entity Type'

    H2020_df_DED51_prc = H2020_df_DED51_prc.loc[H2020_df_DED51_prc["Legal Entity Type"] == "PRC"]

#     ######### FOR COLUMN R Total number of background IPR

#         output_dict["Total number of background IPR"] +=  For_Col_R_and_S(year, IPR_df, 'BACKGROUND', H2020_df, H2020_df_DED51_prc, code)

#     ######### FOR COLUMN S Total number of Foreground PR (of firms)

#         output_dict["Total number of Foreground PR (of firms)"] +=  For_Col_R_and_S(year, IPR_df, 'FOREGROUND', H2020_df, H2020_df_DED51_prc, code)

#     ######### FOR COLUMN T Total number of signed contracts by unique firms

#         output_dict["Total number of signed contracts by unique firms"] +=  For_Col_T(year, H2020_df_DED51_prc)

#     ######### FOR COLUMN U Total co-financing of firms (aggregated per region/year)

#         output_dict["Total co-financing of firms (aggregated per region/year)"] +=  For_Col_U(year, H2020_df_DED51_prc)

#     ######### FOR COLUMN V Total number of patents (single applicant in that region)

#         output_dict["Total number of patents (single applicant in that region)"] +=  For_Col_V_W_and_X(Patent_df, 'Total number of patents (single applicant in that region)', code)

#     ######### FOR COLUMN W Total number of patents

#         output_dict["Total number of patents"] +=  For_Col_V_W_and_X(Patent_df, 'Total number of patents', code)

#     ######### FOR COLUMN X Total number of patents (at least one in that region and at least one from another NUTS 3)

#         output_dict["Total number of patents (at least one in that region and at least one from another NUTS 3)"] +=  For_Col_V_W_and_X(Patent_df, 'Total number of patents (at least one in that region and at least one from another NUTS 3)', code)


# # Round 3

#     ######### FOR COLUMN Y Number of signed contracts by public bodies in projects with firms and/or universities

#         output_dict["Number of signed contracts by public bodies in projects with firms and/or universities"] +=  For_Col_Y(year, H2020_df, H2020_df_DED51_pub, code)

#     ######### FOR COLUMN Z Number of unique participations of regions in projects with firms and/or universities

#         output_dict["Number of unique participations of regions in projects with firms and/or universities"] +=  For_Col_Z(year, H2020_df, H2020_df_DED51_pub, code)


#     ######### FOR COLUMN AA Number of projects of public authorities with background IPR from another region

#         output_dict["Number of projects of public authorities with background IPR from another region"] +=  For_Col_AA_and_AB(year, H2020_df, H2020_df_DED51_pub, IPR_df, 'BACKGROUND', code)

#     ######### FOR COLUMN AB Number of projects of public authorities bringing foreground IPR from another region

#         output_dict["Number of projects of public authorities bringing foreground IPR from another region"] +=  For_Col_AA_and_AB(year, H2020_df, H2020_df_DED51_pub, IPR_df, 'FOREGROUND', code)

#     ######### FOR COLUMN AC Number of projects of public authorities with entrepreneurial bricolage

#         output_dict["Number of projects of public authorities with entrepreneurial bricolage"] +=  For_Col_AC(year, H2020_df, H2020_df_DED51_pub, Brico_df, code)


#     ######### FOR COLUMN AD Number of unique participations of regions in projects with firms from the same region

#         output_dict["Number of unique participations of regions in projects with firms from the same region"] +=  For_Col_AD(year, H2020_df, H2020_df_DED51_pub, code)

#     ######### FOR COLUMN AE Total number of signed contracts by firms

#         output_dict["Total number of signed contracts by firms"] +=  For_Col_AE(year, H2020_df_DED51_prc)

#     ### 1 - Looking for code in column K 'NUTS 3 Code'

#         H2020_df_DED51_hes_and_rec = H2020_df.loc[H2020_df['NUTS 3 Code'] == code]

#     ### 2 - Filtering only Public body from column H 'Legal Entity Type'

#         H2020_df_DED51_hes_and_rec = H2020_df_DED51_hes_and_rec.loc[(H2020_df_DED51_hes_and_rec["Legal Entity Type"] == 'HES') | (H2020_df_DED51_hes_and_rec["Legal Entity Type"] == 'REC')]

#     ######### FOR COLUMN AF Number of signed contracts by universities / research organisations

#         output_dict["Number of signed contracts by universities / research organisations"] +=  For_Col_AF(year, H2020_df_DED51_hes_and_rec)

#     ######### FOR COLUMN AG Budget absorbed by universities / research organisations

#         output_dict["Budget absorbed by universities / research organisations"] +=  For_Col_AG(year, H2020_df_DED51_hes_and_rec)

#     ######### FOR COLUMN AH Difference to highest Degree Centrality (social challenge patents)

#         output_dict["Difference to highest Degree Centrality (social challenge patents)"] +=  For_Col_AH_and_AI(year, H2020_df, H2020_df_DED51_pub, Network_total_df, 'Total social challenge', code)

#     ######### FOR COLUMN AI Difference to highest Degree Centrality (total patents)

#         output_dict["Difference to highest Degree Centrality (total patents)"] +=  For_Col_AH_and_AI(year, H2020_df, H2020_df_DED51_pub, Network_total_df, 'Total patents', code)

#     ######### FOR COLUMN AJ Degree Centrality (social)

#         output_dict["Degree Centrality (social)"] +=  For_Col_AJ_AK_AL_AM_AN_and_AO(Network_total_df, 'Degree Centrality', 'Total social challenge', code)

#     ######### FOR COLUMN AK Betweenness Centrality (social)

#         output_dict["Betweenness Centrality (social)"] +=  For_Col_AJ_AK_AL_AM_AN_and_AO(Network_total_df, 'Betweenness Centrality', 'Total social challenge', code)

#     ######### FOR COLUMN AL Closeness Centrality (social)

#         output_dict["Closeness Centrality (social)"] +=  For_Col_AJ_AK_AL_AM_AN_and_AO(Network_total_df, 'Closeness Centrality', 'Total social challenge', code)

#     ######### FOR COLUMN AM Degree Centrality (total)

#         output_dict["Degree Centrality (total)"] +=  For_Col_AJ_AK_AL_AM_AN_and_AO(Network_total_df, 'Degree Centrality', 'Total patents', code)

#     ######### FOR COLUMN AN Betweenness Centrality (total)

#         output_dict["Betweenness Centrality (total)"] +=  For_Col_AJ_AK_AL_AM_AN_and_AO(Network_total_df, 'Betweenness Centrality', 'Total patents', code)

#     ######### FOR COLUMN AO Closeness Centrality (total)

#         output_dict["Closeness Centrality (total)"] +=  For_Col_AJ_AK_AL_AM_AN_and_AO(Network_total_df, 'Closeness Centrality', 'Total patents', code)

#     ######### FOR COLUMN AP SINGLE Health

#         output_dict["SINGLE Health"] +=  For_Col_AP_to_BC(Patents_Social_df, 'SINGLE Health', code)

#     ######### FOR COLUMN AQ SINGLE Bioeconomy

#         output_dict["SINGLE Bioeconomy"] +=  For_Col_AP_to_BC(Patents_Social_df, 'SINGLE Bioeconomy', code)

#     ######### FOR COLUMN AR SINGLE Energy

#         output_dict["SINGLE Energy"] +=  For_Col_AP_to_BC(Patents_Social_df, 'SINGLE Energy', code)

#     ######### FOR COLUMN AS SINGLE Transport

#         output_dict["SINGLE Transport"] +=  For_Col_AP_to_BC(Patents_Social_df, 'SINGLE Transport', code)

#     ######### FOR COLUMN AT SINGLE Climate

#         output_dict["SINGLE Climate"] +=  For_Col_AP_to_BC(Patents_Social_df, 'SINGLE Climate', code)

#     ######### FOR COLUMN AU SINGLE Security

#         output_dict["SINGLE Security"] +=  For_Col_AP_to_BC(Patents_Social_df, 'SINGLE Security', code)

#     ######### FOR COLUMN AV SINGLE TOTAL

#         output_dict["SINGLE TOTAL"] +=  For_Col_AP_to_BC(Patents_Social_df, 'SINGLE TOTAL', code)

#     ######### FOR COLUMN AW JOINT Health

#         output_dict["JOINT Health"] +=  For_Col_AP_to_BC(Patents_Social_df, 'JOINT Health', code)

#     ######### FOR COLUMN AX JOINT Bioeconomy

#         output_dict["JOINT Bioeconomy"] +=  For_Col_AP_to_BC(Patents_Social_df, 'JOINT Bioeconomy', code)

#     ######### FOR COLUMN AY JOINT Energy

#         output_dict["JOINT Energy"] +=  For_Col_AP_to_BC(Patents_Social_df, 'JOINT Energy', code)

#     ######### FOR COLUMN AZ JOINT Transport

#         output_dict["JOINT Transport"] +=  For_Col_AP_to_BC(Patents_Social_df, 'JOINT Transport', code)

#     ######### FOR COLUMN BA JOINT Climate

#         output_dict["JOINT Climate"] +=  For_Col_AP_to_BC(Patents_Social_df, 'JOINT Climate', code)

#     ######### FOR COLUMN BB JOINT Security

#         output_dict["JOINT Security"] +=  For_Col_AP_to_BC(Patents_Social_df, 'JOINT Security', code)

#     ######### FOR COLUMN BC TOTAL JOINT

#         output_dict["TOTAL JOINT"] +=  For_Col_AP_to_BC(Patents_Social_df, 'TOTAL JOINT', code)


# Round 4

#     ######### FOR COLUMN BD Total co-financing of firms (aggregated per region/year)

    output_dict["Total EU contribution for firms (aggregated per region/year)"] += For_Col_BD(
        year, H2020_df_DED51_prc)

#     ######### FOR COLUMN BE Total budget of firms (aggregated per region/year)

    output_dict["Total budget of firms (aggregated per region/year)"] += For_Col_BE(
        year, H2020_df_DED51_prc)

#     ######### FOR COLUMN BF Number of projects with firms exits

    output_dict["Number of projects with firms exits"] += For_Col_BF(
        year, H2020_df_DED51_pub)

#     ######### FOR COLUMN BG Difference to highest Degree Centrality (social challenge patents)

    output_dict["Difference to highest Degree Centrality (social challenge patents)"] += For_Col_BG_and_BH(
        year, H2020_df, H2020_df_DED51_pub, Network_total_df, 'Total social challenge', code)

#     ######### FOR COLUMN BH Difference to highest Degree Centrality (total patents)

    output_dict["Difference to highest Degree Centrality (total patents)"] += For_Col_BG_and_BH(
        year, H2020_df, H2020_df_DED51_pub, Network_total_df, 'Total patents', code)

    return output_dict


def Extract_Year_From_Dates(df, col_name, year_arr):
    curr_year = datetime.datetime.now().year

    for i, val in enumerate(df[str(col_name)]):
        try:
            b = datetime.datetime.strptime(val, '%d/%m/%Y').year
            if (not b in year_arr) and (b <= curr_year):
                # print(b)
                year_arr.append(int(b))
        except TypeError:
            pass

    return year_arr


def For_Col_K(year_arr, df):
    row_count = []
    for year in year_arr:
        print("For ", year)

        df1 = df[(df['Project Start Date'].dt.strftime(
            '%d-%m-%Y') == '01-01-' + str(year))]

        df2 = df[(df['Project Start Date'].dt.strftime('%Y') < str(year)) & (
            df['Project End Date'].dt.strftime('%Y') >= str(year))]

        result_df = Conact_DFs(df1, df2)
        row_count.append(len(result_df.index))

    return row_count


def For_Col_L(year_arr, df):
    row_count = []
    for year in year_arr:
        print("For ", year)

        df1 = df[(df['Project Start Date'].dt.strftime(
            '%d-%m-%Y') == '01-01-' + str(year))]

        df2 = df[(df['Project Start Date'].dt.strftime('%Y') < str(year)) & (
            df['Project End Date'].dt.strftime('%Y') >= str(year))]

        result_df = Conact_DFs(df1, df2)
        result_df = result_df.drop_duplicates(subset=['Project ID'])
        # print(len(result_df.index), "faraz")
        row_count.append(len(result_df.index))

    return row_count


def For_Col_M(year_arr, df):
    unique_months = []
    for year in year_arr:
        print("For ", year)

        df2 = df[(df['Project Start Date'].dt.strftime('%Y') <= str(year))]
        prev_df = df2
        df2 = Conact_DFs(prev_df, df2)
        df2 = df2.drop_duplicates(subset=['Project ID'], keep='first')
        print(df2)

        no_of_months = 0

        for index, row in df2.iterrows():

            print(row['Project Start Date'])
            print(row['Project End Date'])

            if pd.to_datetime(row['Project End Date']).year > int(year):
                no_of_months += (datetime.datetime(year, 12, 31) -
                                 row['Project Start Date']) / np.timedelta64(1, 'M')
                print(round(no_of_months))
                no_of_months = round(no_of_months)

            elif pd.to_datetime(row['Project End Date']).year <= int(year):
                no_of_months += (row['Project End Date'] -
                                 row['Project Start Date']) / np.timedelta64(1, 'M')
                print(round(no_of_months))
                no_of_months = round(no_of_months)

        unique_months.append(no_of_months)

    print(unique_months)

    return unique_months


def For_Col_N(year_arr, df):
    eu_contribution_count = []
    for year in year_arr:
        print("For ", year)

        df1 = df[(df['Contract signature date'].dt.strftime('%Y') == str(year))]
        print(df1)

        total = df1['EU Contribution (€)'].apply(
            lambda x: pd.to_numeric(x, errors='coerce')).sum()

        print(total)

        eu_contribution_count.append(total)

    return eu_contribution_count


def For_Col_O(year_arr, df):
    eu_contribution_count = []
    for year in year_arr:
        print("For ", year)

        df1 = df[(df['Contract signature date'].dt.strftime('%Y') == str(year))]
        print(df1)

        total_eu = df1['EU Contribution (€)'].apply(
            lambda x: pd.to_numeric(x, errors='coerce')).sum()
        total_tc = df1['H2020 Total Cost'].apply(
            lambda x: pd.to_numeric(x, errors='coerce')).sum()

        total = total_tc - total_eu

        print(total)

        eu_contribution_count.append(total)

    return eu_contribution_count


def For_Col_P_and_Q(year_arr, IPR_df: pd.DataFrame, IPR_Type,  H2020_df: pd.DataFrame, H2020_df_DED51_pub: pd.DataFrame, code):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    H2020_df_DED51_pub = H2020_df_DED51_pub.sort_values('Project ID')
    H2020_pub_Uid_df = H2020_df_DED51_pub.drop_duplicates(
        'Project ID', keep='first')
    print(H2020_pub_Uid_df)

    for h2020_index, h2020_row in H2020_pub_Uid_df.iterrows():

        project_id = h2020_row['Project ID']
        print(project_id)

        IPR_df_Filtered = IPR_df.loc[IPR_df["project ID"] == project_id]
        print(IPR_df_Filtered)

        if not IPR_df_Filtered.empty:
            IPR_df_Filtered = IPR_df_Filtered.loc[IPR_df_Filtered["IPR"] == IPR_Type]
            print(IPR_df_Filtered)

            if not IPR_df_Filtered.empty:
                IPR_df_Filtered.drop_duplicates(
                    subset=['patentFamilyIdentifier'], keep='last', inplace=True)
                print(IPR_df_Filtered)

                if not IPR_df_Filtered.empty:
                    for IPR_df_index, IPR_df_row in IPR_df_Filtered.iterrows():

                        H2020_Org_Id_Filtered_df = H2020_df.loc[(H2020_df["Project ID"] == IPR_df_row["project ID"]) & (
                            H2020_df["Organisation ID"] == IPR_df_row["Organisation ID"])]

                        if not H2020_Org_Id_Filtered_df.empty:
                            print(H2020_Org_Id_Filtered_df)

                            for H2020_Org_Id_Filtered_df_index, row_1 in H2020_Org_Id_Filtered_df.iterrows():

                                if row_1['NUTS 3 Code'] != code:

                                    project_start_year = row_1['Project Start Date'].year
                                    print(project_start_year)

                                    if project_start_year in year_arr:
                                        index = year_arr.index(
                                            project_start_year)
                                        patt_count_array[index +
                                                         1] = patt_count_array[index + 1] + 1
                                        print(patt_count_array[index + 1])

                                    else:
                                        continue
                                else:
                                    continue

    return patt_count_array


def For_Col_R_and_S(year_arr, IPR_df, IPR_Type,  H2020_df, H2020_df_DED51_prc, code):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    H2020_df_DED51_prc = H2020_df_DED51_prc.sort_values('Organisation ID')

    H2020_df_DED51_prc = H2020_df_DED51_prc.drop_duplicates(
        'Organisation ID', keep='first')
    print(H2020_df_DED51_prc)

    for h2020_index, h2020_row in H2020_df_DED51_prc.iterrows():

        org_id = h2020_row['Organisation ID']
        print(org_id)

        IPR_df_Filtered = IPR_df.loc[IPR_df["Organisation ID"] == org_id]
        print(IPR_df_Filtered)

        if not IPR_df_Filtered.empty:
            IPR_df_Filtered = IPR_df_Filtered.loc[IPR_df_Filtered["IPR"] == IPR_Type]
            print(IPR_df_Filtered)

            IPR_df_Filtered = IPR_df_Filtered.sort_values('priority date')
            print(IPR_df_Filtered)

            if not IPR_df_Filtered.empty:
                IPR_df_Filtered.drop_duplicates(
                    subset=['patentFamilyIdentifier'], keep='first', inplace=True)
                print(IPR_df_Filtered)
                print(IPR_df_Filtered['priority date'])

                if not IPR_df_Filtered.empty:
                    for IPR_df_index, IPR_df_row in IPR_df_Filtered.iterrows():

                        if (not pd.isna(IPR_df_row["priority date"])) and (IPR_df_row["priority date"] != "NaT"):

                            print(IPR_df_row["priority date"])

                            priorty_year = datetime.datetime.strptime(
                                IPR_df_row['priority date'], '%Y-%m-%d').year

                            print(priorty_year)

                            if priorty_year in year_arr:
                                index = year_arr.index(priorty_year)
                                patt_count_array[index] = patt_count_array[index] + 1
                                print(patt_count_array)

                        elif (not pd.isna(IPR_df_row["application date"])) and (IPR_df_row["application date"] != "NaT"):

                            print(IPR_df_row["application date"])

                            priorty_year = datetime.datetime.strptime(
                                IPR_df_row['application date'], '%Y-%m-%d').year

                            print(priorty_year)

                            if priorty_year in year_arr:
                                index = year_arr.index(priorty_year)
                                patt_count_array[index] = patt_count_array[index] + 1
                                print(patt_count_array[index])

                        else:
                            continue

    return patt_count_array


def For_Col_T(year_arr, H2020_df_DED51_prc: pd.DataFrame):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    H2020_df_DED51_prc = H2020_df_DED51_prc.sort_values(
        'Contract signature date')
    print(H2020_df_DED51_prc['Contract signature date'])

    for year in year_arr:

        print(year)

        H2020_df_Filtered_by_Year = H2020_df_DED51_prc[
            H2020_df_DED51_prc['Contract signature date'].dt.year == year]
        print(H2020_df_Filtered_by_Year['Contract signature date'])

        if not H2020_df_Filtered_by_Year.empty:

            H2020_prc_Uid_df = H2020_df_Filtered_by_Year.drop_duplicates(
                'Organisation ID', keep='first')
            print(H2020_prc_Uid_df)

            row_count = H2020_prc_Uid_df.shape[0]
            print(row_count)

            index = year_arr.index(year)
            patt_count_array[index] = row_count
            print(patt_count_array)

        else:
            continue

    return patt_count_array


def For_Col_U(year_arr, H2020_df_DED51_prc: pd.DataFrame):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    H2020_df_DED51_prc = H2020_df_DED51_prc.sort_values(
        'Contract signature date')
    print(H2020_df_DED51_prc['Contract signature date'])

    for year in year_arr:

        print(year)

        H2020_df_Filtered_by_Year = H2020_df_DED51_prc[
            H2020_df_DED51_prc['Contract signature date'].dt.year == year]
        print(H2020_df_Filtered_by_Year['Contract signature date'])

        if not H2020_df_Filtered_by_Year.empty:
            sum = 0

            for H2020_df_prc_index, H2020_df_prc_row in H2020_df_Filtered_by_Year.iterrows():

                sub = H2020_df_prc_row['H2020 Total Cost'] - \
                    H2020_df_prc_row['H2020 Net EU Contribution']
                sum = sum + sub

            index = year_arr.index(year)
            patt_count_array[index] = sum
            print(patt_count_array)

        else:
            continue

    return patt_count_array


def For_Col_V_W_and_X(Patent_df: pd.DataFrame, column_name, code):

    patt_count_array = ['', '', '', '', '', '', '', '', '']

    Patent_df_Filtered = Patent_df.loc[Patent_df['NUTS 3 Code'] == code]

    i = 0
    for Patent_df_index, Patent_df_row in Patent_df_Filtered.iterrows():

        val = Patent_df_row[column_name]
        patt_count_array[i] = val
        i = i + 1

    return patt_count_array


def For_Col_Y(year_arr, H2020_df: pd.DataFrame, H2020_df_DED51_pub: pd.DataFrame, code):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    for year in year_arr:
        print(year)

        H2020_df_filtered_by_Contract_Sig_Date = H2020_df_DED51_pub[(
            H2020_df_DED51_pub['Contract signature date'].dt.strftime('%Y') == str(year))]
        print(H2020_df_filtered_by_Contract_Sig_Date)

        H2020_df_filtered_by_Contract_Sig_Date_unq = H2020_df_filtered_by_Contract_Sig_Date.drop_duplicates(
            subset='Project ID', keep="first")
        print(H2020_df_filtered_by_Contract_Sig_Date_unq)

        for index, row in H2020_df_filtered_by_Contract_Sig_Date_unq.iterrows():

            project_id = row['Project ID']
            print(project_id)

            H2020_df_filtered = H2020_df.loc[H2020_df["Project ID"]
                                             == project_id]
            print(H2020_df_filtered)

            if not H2020_df_filtered.empty:
                H2020_df_filtered = H2020_df_filtered[(H2020_df_filtered["Legal Entity Type"] == 'PRC') | (
                    H2020_df_filtered["Legal Entity Type"] == 'HES') | (H2020_df_filtered["Legal Entity Type"] == 'REC')]
                print(H2020_df_filtered)

                if not H2020_df_filtered.empty:
                    H2020_df_filtered = H2020_df_filtered[H2020_df_filtered["NUTS 3 Code"] != code]
                    print(H2020_df_filtered)

                    if not H2020_df_filtered.empty:
                        H2020_df_filtered_by_Contract_Sig_Date_project_id = H2020_df_filtered_by_Contract_Sig_Date.loc[
                            H2020_df["Project ID"] == project_id]
                        print(H2020_df_filtered_by_Contract_Sig_Date_project_id)

                        count_row = H2020_df_filtered_by_Contract_Sig_Date_project_id.shape[0]
                        print(count_row)

                        index = year_arr.index(year)
                        patt_count_array[index] += count_row
                        print(patt_count_array)
                    else:
                        continue

                else:
                    continue

            else:
                continue

    return patt_count_array


def For_Col_Z(year_arr, H2020_df: pd.DataFrame, H2020_df_DED51_pub: pd.DataFrame, code):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    for year in year_arr:
        print(year)

        H2020_df_filtered_by_Contract_Sig_Date = H2020_df_DED51_pub[(
            H2020_df_DED51_pub['Contract signature date'].dt.strftime('%Y') == str(year))]
        print(H2020_df_filtered_by_Contract_Sig_Date)

        H2020_df_filtered_by_Contract_Sig_Date_unq = H2020_df_filtered_by_Contract_Sig_Date.drop_duplicates(
            subset='Project ID', keep="first")
        print(H2020_df_filtered_by_Contract_Sig_Date_unq)

        for index, row in H2020_df_filtered_by_Contract_Sig_Date_unq.iterrows():

            project_id = row['Project ID']
            print(project_id)

            H2020_df_filtered = H2020_df.loc[H2020_df["Project ID"]
                                             == project_id]
            print(H2020_df_filtered)

            if not H2020_df_filtered.empty:
                H2020_df_filtered = H2020_df_filtered[(H2020_df_filtered["Legal Entity Type"] == 'PRC') | (
                    H2020_df_filtered["Legal Entity Type"] == 'HES') | (H2020_df_filtered["Legal Entity Type"] == 'REC')]
                print(H2020_df_filtered)

                if not H2020_df_filtered.empty:
                    H2020_df_filtered = H2020_df_filtered[H2020_df_filtered["NUTS 3 Code"] != code]
                    print(H2020_df_filtered)

                    if not H2020_df_filtered.empty:
                        H2020_df_filtered_by_Contract_Sig_Date_project_id = H2020_df_filtered_by_Contract_Sig_Date_unq.loc[
                            H2020_df["Project ID"] == project_id]
                        print(H2020_df_filtered_by_Contract_Sig_Date_project_id)

                        count_row = H2020_df_filtered_by_Contract_Sig_Date_project_id.shape[0]
                        print(count_row)

                        index = year_arr.index(year)
                        patt_count_array[index] += count_row
                        print(patt_count_array)
                    else:
                        continue
                else:
                    continue
            else:
                continue

    return patt_count_array


def For_Col_AA_and_AB(year_arr, H2020_df: pd.DataFrame, H2020_df_DED51_pub: pd.DataFrame, IPR_df: pd.DataFrame, IPR_Type, code):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    for year in year_arr:
        print(year)

        H2020_df_filtered_by_Contract_Sig_Date = H2020_df_DED51_pub[(
            H2020_df_DED51_pub['Contract signature date'].dt.strftime('%Y') == str(year))]
        print(H2020_df_filtered_by_Contract_Sig_Date)

        H2020_df_filtered_by_Contract_Sig_Date_unq = H2020_df_filtered_by_Contract_Sig_Date.drop_duplicates(
            subset='Project ID', keep="first")
        print(H2020_df_filtered_by_Contract_Sig_Date_unq)

        for index, row in H2020_df_filtered_by_Contract_Sig_Date_unq.iterrows():

            project_id = row['Project ID']
            print(project_id)

            H2020_df_filtered = H2020_df.loc[H2020_df["Project ID"]
                                             == project_id]
            print(H2020_df_filtered)

            if not H2020_df_filtered.empty:
                H2020_df_filtered = H2020_df_filtered[(H2020_df_filtered["Legal Entity Type"] == 'PRC') | (
                    H2020_df_filtered["Legal Entity Type"] == 'HES') | (H2020_df_filtered["Legal Entity Type"] == 'REC')]
                print(H2020_df_filtered)

                if not H2020_df_filtered.empty:
                    H2020_df_filtered = H2020_df_filtered[H2020_df_filtered["NUTS 3 Code"] != code]
                    print(H2020_df_filtered)

                    if not H2020_df_filtered.empty:
                        IPR_df_Filtered = IPR_df.loc[IPR_df["project ID"]
                                                     == project_id]
                        print(IPR_df_Filtered)

                        if not IPR_df_Filtered.empty:
                            IPR_df_Filtered = IPR_df_Filtered.loc[IPR_df_Filtered["IPR"] == IPR_Type]
                            print(IPR_df_Filtered)

                            if not IPR_df_Filtered.empty:
                                print(IPR_df_Filtered)

                                count_row = 1
                                print(count_row)

                                index = year_arr.index(year)
                                patt_count_array[index] += count_row
                                print(patt_count_array)
                            else:
                                continue
                        else:
                            continue
                    else:
                        continue
                else:
                    continue
            else:
                continue

    return patt_count_array


def For_Col_AC(year_arr, H2020_df: pd.DataFrame, H2020_df_DED51_pub: pd.DataFrame, Brico_df, code):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    for year in year_arr:
        print(year)

        H2020_df_filtered_by_Contract_Sig_Date = H2020_df_DED51_pub[(
            H2020_df_DED51_pub['Contract signature date'].dt.strftime('%Y') == str(year))]
        print(H2020_df_filtered_by_Contract_Sig_Date)

        H2020_df_filtered_by_Contract_Sig_Date_unq = H2020_df_filtered_by_Contract_Sig_Date.drop_duplicates(
            subset='Project ID', keep="first")
        print(H2020_df_filtered_by_Contract_Sig_Date_unq)

        for index, row in H2020_df_filtered_by_Contract_Sig_Date_unq.iterrows():

            project_id = row['Project ID']
            print(project_id)

            H2020_df_filtered = H2020_df.loc[H2020_df["Project ID"]
                                             == project_id]
            print(H2020_df_filtered)

            if not H2020_df_filtered.empty:
                H2020_df_filtered = H2020_df_filtered[(H2020_df_filtered["Legal Entity Type"] == 'PRC') | (
                    H2020_df_filtered["Legal Entity Type"] == 'HES') | (H2020_df_filtered["Legal Entity Type"] == 'REC')]
                print(H2020_df_filtered)

                if not H2020_df_filtered.empty:
                    H2020_df_filtered = H2020_df_filtered[H2020_df_filtered["NUTS 3 Code"] != code]
                    print(H2020_df_filtered)

                    if project_id in Brico_df['Project ID'].values:
                        H2020_df_filtered_by_Contract_Sig_Date_project_id = H2020_df_filtered_by_Contract_Sig_Date_unq.loc[
                            H2020_df["Project ID"] == project_id]
                        print(H2020_df_filtered_by_Contract_Sig_Date_project_id)

                        count_row = H2020_df_filtered_by_Contract_Sig_Date_project_id.shape[0]
                        print(count_row)

                        index = year_arr.index(year)
                        patt_count_array[index] += count_row
                        print(patt_count_array)
                    else:
                        continue
                else:
                    continue
            else:
                continue

    return patt_count_array


def For_Col_AD(year_arr, H2020_df: pd.DataFrame, H2020_df_DED51_pub: pd.DataFrame, code):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    for year in year_arr:
        print(year)

        H2020_df_filtered_by_Contract_Sig_Date = H2020_df_DED51_pub[(
            H2020_df_DED51_pub['Contract signature date'].dt.strftime('%Y') == str(year))]
        print(H2020_df_filtered_by_Contract_Sig_Date)

        H2020_df_filtered_by_Contract_Sig_Date_unq = H2020_df_filtered_by_Contract_Sig_Date.drop_duplicates(
            subset='Project ID', keep="first")
        print(H2020_df_filtered_by_Contract_Sig_Date_unq)

        for index, row in H2020_df_filtered_by_Contract_Sig_Date_unq.iterrows():

            project_id = row['Project ID']
            print(project_id)

            H2020_df_filtered = H2020_df.loc[H2020_df["Project ID"]
                                             == project_id]
            print(H2020_df_filtered)

            if not H2020_df_filtered.empty:
                H2020_df_filtered = H2020_df_filtered[(
                    H2020_df_filtered['Contract signature date'].dt.strftime('%Y') == str(year))]
                print(H2020_df_filtered)

                if not H2020_df_filtered.empty:
                    H2020_df_filtered = H2020_df_filtered[(
                        H2020_df_filtered["Legal Entity Type"] == 'PRC')]
                    print(H2020_df_filtered)

                    if not H2020_df_filtered.empty:
                        H2020_df_filtered = H2020_df_filtered[H2020_df_filtered["NUTS 3 Code"] == code]
                        print(H2020_df_filtered)

                        if not H2020_df_filtered.empty:
                            H2020_df_filtered_by_Contract_Sig_Date_project_id = H2020_df_filtered_by_Contract_Sig_Date_unq.loc[
                                H2020_df["Project ID"] == project_id]
                            print(H2020_df_filtered_by_Contract_Sig_Date_project_id)

                            count_row = H2020_df_filtered_by_Contract_Sig_Date_project_id.shape[0]
                            print(count_row)

                            index = year_arr.index(year)
                            patt_count_array[index] += count_row
                            print(patt_count_array)
                        else:
                            continue
                    else:
                        continue
                else:
                    continue
            else:
                continue

    return patt_count_array


def For_Col_AE(year_arr, H2020_df_DED51_prc: pd.DataFrame):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    H2020_df_DED51_prc = H2020_df_DED51_prc.sort_values('Organisation ID')

    for year in year_arr:
        print(year)

        H2020_df_DED51_prc_filtered = H2020_df_DED51_prc[(
            H2020_df_DED51_prc['Contract signature date'].dt.strftime('%Y') == str(year))]
        print(H2020_df_DED51_prc_filtered)

        count_row = H2020_df_DED51_prc_filtered.shape[0]
        print(count_row)

        index = year_arr.index(year)
        patt_count_array[index] += count_row
        print(patt_count_array)

    return patt_count_array


def For_Col_AF(year_arr, H2020_df_DED51_hes_and_rec: pd.DataFrame):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    H2020_df_DED51_hes_and_rec = H2020_df_DED51_hes_and_rec.sort_values(
        'Organisation ID')

    for year in year_arr:
        print(year)

        H2020_df_DED51_prc_filtered = H2020_df_DED51_hes_and_rec[(
            H2020_df_DED51_hes_and_rec['Contract signature date'].dt.strftime('%Y') == str(year))]
        print(H2020_df_DED51_prc_filtered)

        count_row = H2020_df_DED51_prc_filtered.shape[0]
        print(count_row)

        index = year_arr.index(year)
        patt_count_array[index] += count_row
        print(patt_count_array)

    return patt_count_array


def For_Col_AG(year_arr, H2020_df_DED51_hes_and_rec: pd.DataFrame):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    H2020_df_DED51_hes_and_rec = H2020_df_DED51_hes_and_rec.sort_values(
        'Organisation ID')

    for year in year_arr:
        print(year)

        H2020_df_DED51_prc_filtered = H2020_df_DED51_hes_and_rec[(
            H2020_df_DED51_hes_and_rec['Contract signature date'].dt.strftime('%Y') == str(year))]
        print(H2020_df_DED51_prc_filtered)

        sum_of_col = H2020_df_DED51_prc_filtered['H2020 Net EU Contribution'].sum(
        )
        print(sum_of_col)

        index = year_arr.index(year)
        patt_count_array[index] += sum_of_col
        print(patt_count_array)

    return patt_count_array


def For_Col_AH_and_AI(year_arr, H2020_df: pd.DataFrame, H2020_df_DED51_pub: pd.DataFrame, Network_total_df: pd.DataFrame, Field_type, code):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    for year in year_arr:
        print(year)

        H2020_df_filtered_by_Contract_Sig_Date = H2020_df_DED51_pub[(
            H2020_df_DED51_pub['Contract signature date'].dt.strftime('%Y') == str(year))]

        H2020_df_filtered_by_Contract_Sig_Date_unq = H2020_df_filtered_by_Contract_Sig_Date.drop_duplicates(
            subset='Project ID', keep="first")
        print(H2020_df_filtered_by_Contract_Sig_Date_unq)

        H2020_uniq_proj_id_list = Create_List_Of_Unique_Values(
            H2020_df_filtered_by_Contract_Sig_Date_unq['Project ID'])
        print(H2020_uniq_proj_id_list)

        H2020_df_filtered = H2020_df[H2020_df["Project ID"].isin(
            H2020_uniq_proj_id_list)]

        if not H2020_df_filtered.empty:
            H2020_df_filtered = H2020_df_filtered[H2020_df_filtered["NUTS 3 Code"] != code]

            if not H2020_df_filtered.empty:
                H2020_df_filtered = H2020_df_filtered[(H2020_df_filtered["Legal Entity Type"] == 'PRC') | (
                    H2020_df_filtered["Legal Entity Type"] == 'HES') | (H2020_df_filtered["Legal Entity Type"] == 'REC')]
                print(H2020_df_filtered)

                if not H2020_df_filtered.empty:
                    print(H2020_df_filtered)

                    H2020_uniq_nuts3_code_list = Create_List_Of_Unique_Values(
                        H2020_df_filtered['NUTS 3 Code'])
                    print(H2020_uniq_nuts3_code_list)

                    rex = re.compile('[^A-Za-z0-9]+')
                    H2020_uniq_nuts3_code_list_filtered = [
                        x for x in H2020_uniq_nuts3_code_list if not rex.match(x)]
                    print(H2020_uniq_nuts3_code_list_filtered)

                    Network_total_df_filtered = Network_total_df[Network_total_df["NUTS 3 Code"].isin(
                        H2020_uniq_nuts3_code_list_filtered)]

                    if not Network_total_df_filtered.empty:
                        Network_total_df_filtered = Network_total_df_filtered[(
                            Network_total_df_filtered["Field"] == Field_type)]

                        if not Network_total_df_filtered.empty:
                            Network_total_df_filtered = Network_total_df_filtered[
                                Network_total_df_filtered['Year'] == year]
                            print(Network_total_df_filtered)

                            if not Network_total_df_filtered.empty:

                                max_val_from_filtered_network_df = Network_total_df_filtered['Degree Centrality'].max(
                                )
                                print(max_val_from_filtered_network_df)

                                Network_total_df_code_based = Network_total_df[(
                                    Network_total_df["NUTS 3 Code"] == code)]

                                Network_total_df_code_based = Network_total_df_code_based[(
                                    Network_total_df_code_based["Field"] == Field_type)]

                                Network_total_df_code_based = Network_total_df_code_based[(
                                    Network_total_df_code_based['Year'] == year)]
                                print(Network_total_df_code_based)

                                max_val_from_code_network_df = Network_total_df_code_based['Degree Centrality'].max(
                                )
                                print(max_val_from_code_network_df)

                                sub_value = max_val_from_filtered_network_df - max_val_from_code_network_df
                                print(sub_value)

                                index = year_arr.index(year)
                                patt_count_array[index] = sub_value
                                print(patt_count_array)

                            else:
                                continue
                        else:
                            continue
                    else:
                        continue
            else:
                continue

        else:
            continue

    return patt_count_array


def For_Col_AJ_AK_AL_AM_AN_and_AO(Network_total_df: pd.DataFrame, column_name, Field_type, code):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    Network_total_df_Filtered = Network_total_df.loc[Network_total_df['NUTS 3 Code'] == code]
    Network_total_df_Filtered = Network_total_df_Filtered.loc[
        Network_total_df_Filtered['Field'] == Field_type]

    i = 0
    for Network_total_df_Filtered_index, Network_total_df_Filtered_row in Network_total_df_Filtered.iterrows():

        val = Network_total_df_Filtered_row[column_name]
        patt_count_array[i] = val
        i = i + 1

    print(patt_count_array)
    return patt_count_array


def For_Col_AP_to_BC(Patents_Social_df: pd.DataFrame, column_name, code):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    Patents_Social_df_Filtered = Patents_Social_df.loc[Patents_Social_df['NUTS 3 Code'] == code]

    i = 0
    for Patents_Social_df_Filtered_index, Patents_Social_df_Filtered_row in Patents_Social_df_Filtered.iterrows():

        val = Patents_Social_df_Filtered_row[column_name]
        patt_count_array[i] = val
        i = i + 1

    print(patt_count_array)
    return patt_count_array


def For_Col_BD(year_arr, H2020_df_DED51_prc: pd.DataFrame):

    eu_contribution_count = []
    for year in year_arr:
        print("For ", year)

        df1 = H2020_df_DED51_prc[(
            H2020_df_DED51_prc['Contract signature date'].dt.strftime('%Y') == str(year))]
        print(df1)

        total_eu = df1['EU Contribution (€)'].apply(
            lambda x: pd.to_numeric(x, errors='coerce')).sum()

        print(total_eu)

        eu_contribution_count.append(total_eu)

    return eu_contribution_count


def For_Col_BE(year_arr, H2020_df_DED51_prc: pd.DataFrame):

    h2020_total_cost_count = []
    for year in year_arr:
        print("For ", year)

        df1 = H2020_df_DED51_prc[(
            H2020_df_DED51_prc['Contract signature date'].dt.strftime('%Y') == str(year))]
        print(df1)

        total_cost_sum = df1['H2020 Total Cost'].apply(
            lambda x: pd.to_numeric(x, errors='coerce')).sum()

        print(total_cost_sum)

        h2020_total_cost_count.append(total_cost_sum)

    return h2020_total_cost_count


def For_Col_BF(year_arr, H2020_df_DED51_pub: pd.DataFrame):

    h2020_exit_count = []
    for year in year_arr:
        print("For ", year)

        df1 = H2020_df_DED51_pub[(
            H2020_df_DED51_pub['Contract signature date'].dt.strftime('%Y') == str(year))]
        print(df1)

        H2020_df_filtered_by_Project_ID_unq = df1.drop_duplicates(
            subset='Project ID', keep="first")

        print(H2020_df_filtered_by_Project_ID_unq)

        exit_count = (df1['Exit (1=exit)'] == 1).sum()
        print(exit_count)

        h2020_exit_count.append(exit_count)

    print(h2020_exit_count)
    return h2020_exit_count


def For_Col_BG_and_BH(year_arr, H2020_df: pd.DataFrame, H2020_df_DED51_pub: pd.DataFrame, Network_total_df: pd.DataFrame, Field_type, code):

    patt_count_array = [0, 0, 0, 0, 0, 0, 0, 0, 0]

    for year in year_arr:
        print(year)

        H2020_df_filtered_by_Contract_Sig_Date = H2020_df_DED51_pub[(
            H2020_df_DED51_pub['Contract signature date'].dt.strftime('%Y') == str(year))]

        H2020_df_filtered_by_Contract_Sig_Date_unq = H2020_df_filtered_by_Contract_Sig_Date.drop_duplicates(
            subset='Project ID', keep="first")
        print(H2020_df_filtered_by_Contract_Sig_Date_unq)

        H2020_uniq_proj_id_list = Create_List_Of_Unique_Values(
            H2020_df_filtered_by_Contract_Sig_Date_unq['Project ID'])
        print(H2020_uniq_proj_id_list)

        H2020_df_filtered = H2020_df[H2020_df["Project ID"].isin(
            H2020_uniq_proj_id_list)]

        if not H2020_df_filtered.empty:
            H2020_df_filtered = H2020_df_filtered[H2020_df_filtered["NUTS 3 Code"] != code]

            if not H2020_df_filtered.empty:
                H2020_df_filtered = H2020_df_filtered[(H2020_df_filtered["Legal Entity Type"] == 'PRC') | (
                    H2020_df_filtered["Legal Entity Type"] == 'HES') | (H2020_df_filtered["Legal Entity Type"] == 'REC')]
                print(H2020_df_filtered)

                if not H2020_df_filtered.empty:
                    print(H2020_df_filtered)

                    H2020_uniq_nuts3_code_list = Create_List_Of_Unique_Values(
                        H2020_df_filtered['NUTS 3 Code'])
                    print(H2020_uniq_nuts3_code_list)

                    rex = re.compile('[^A-Za-z0-9]+')
                    H2020_uniq_nuts3_code_list_filtered = [
                        x for x in H2020_uniq_nuts3_code_list if not rex.match(x)]
                    print(H2020_uniq_nuts3_code_list_filtered)

                    Network_total_df_filtered = Network_total_df[Network_total_df["NUTS 3 Code"].isin(
                        H2020_uniq_nuts3_code_list_filtered)]

                    if not Network_total_df_filtered.empty:
                        Network_total_df_filtered = Network_total_df_filtered[
                            Network_total_df_filtered['Year'] == year]
                        print(Network_total_df_filtered)

                        if not Network_total_df_filtered.empty:
                            avg_val_from_filtered_network_df = Network_total_df_filtered['Degree Centrality'].mean(
                            )
                            print(avg_val_from_filtered_network_df)

                            Network_total_df_code_based = Network_total_df[(
                                Network_total_df["NUTS 3 Code"] == code)]

                            Network_total_df_code_based = Network_total_df_code_based[(
                                Network_total_df_code_based["Field"] == Field_type)]

                            Network_total_df_code_based = Network_total_df_code_based[(
                                Network_total_df_code_based['Year'] == year)]
                            print(Network_total_df_code_based)

                            avg_val_from_code_network_df = Network_total_df_code_based['Degree Centrality'].mean(
                            )
                            print(avg_val_from_code_network_df)

                            sub_value = avg_val_from_filtered_network_df - avg_val_from_code_network_df
                            print(sub_value)

                            index = year_arr.index(year)
                            patt_count_array[index] = sub_value
                            print(patt_count_array)
                        else:
                            continue
                    else:
                        continue
                else:
                    continue
            else:
                continue

        else:
            continue

    return patt_count_array


# NUTS3_file_path, IPR_file_path, abs_path):
def main(NUTS3_file_path, H2020_file_path, IPR_file_path, abs_path, Patents_file_path, Brico_file_path, Network_total_file_path, Patents_Social_file_path):

    # FOR COLUMN K for Total number of public bodies participations, running for at least one year

    # Opening H2020.xlsx
    H2020_df = Create_DF(H2020_file_path)

    # Opening IPR.xlsx
    IPR_df = Create_DF(IPR_file_path)

    # IPR_df = Clean_IPR_Df_Data(IPR_df, H2020_df, IPR_file_path, abs_path)

    # Opening Patents.xlsx
    Patent_df = Create_DF_from_CSV(Patents_file_path)

    # Opening Brico.xlsx
    Brico_df = Create_DF(Brico_file_path)

    # Opening Network_total.xlsx
    Network_total_df = Create_DF(Network_total_file_path)

    # Opening Patents_Social.xlsx
    Patents_Social_df = Create_DF(Patents_Social_file_path)


# columns
    output_dict = {

        'Country': [], 'NUTS3': [], 'Urban-rural': [], 'Metropolitan': [], 'urban-rural remoteness': [], 'Border region': [], 'Coastal region': [], 'Mountain region': [], 'Island': [], 'Year': [],

        # 'Total number of  public bodies participations, running for at least one year': [],
        # 'Total number of unique  projects involving public bodies, running for at least one year': [],
        # 'Number project months acummulating  since 2014': [],
        # 'Multiannual EU funds commitment benefitting   public bodies aggregated at regional level (date of contract signature  counts)': [],
        # 'Multiannual self financing  commitment benefitting   public bodies aggregated at regional level (date of contract signature  counts)': [],
        # 'Total number of background IPR from another region accessed via a project that has been running 1 year (counted once per project lifetime)': [],
        # 'Total number of foreground IPR from another region accessed via a project that has been running 1 year (counted once per project lifetime)': [],

        # 'Total number of background IPR': [],
        # 'Total number of Foreground PR (of firms)': [],
        # 'Total number of signed contracts by unique firms': [],
        # 'Total co-financing of firms (aggregated per region/year)': [],
        # 'Total number of patents (single applicant in that region)': [],
        # 'Total number of patents': [],
        # 'Total number of patents (at least one in that region and at least one from another NUTS 3)': [],

        # 'Number of signed contracts by public bodies in projects with firms and/or universities': [],
        # 'Number of unique participations of regions in projects with firms and/or universities': [],

        # 'Number of projects of public authorities with background IPR from another region': [],
        # 'Number of projects of public authorities bringing foreground IPR from another region': [],
        # 'Number of projects of public authorities with entrepreneurial bricolage': [],
        # 'Number of unique participations of regions in projects with firms from the same region': [],
        # 'Total number of signed contracts by firms': [],
        # 'Number of signed contracts by universities / research organisations': [],
        # 'Budget absorbed by universities / research organisations': [],
        # 'Difference to highest Degree Centrality (social challenge patents)': [],
        # 'Difference to highest Degree Centrality (total patents)' : [],

        # 'Degree Centrality (social)': [],
        # 'Betweenness Centrality (social)': [],
        # 'Closeness Centrality (social)': [],
        # 'Degree Centrality (total)': [],
        # 'Betweenness Centrality (total)': [],
        # 'Closeness Centrality (total)': [],

        # 'SINGLE Health': [],
        # 'SINGLE Bioeconomy': [],
        # 'SINGLE Energy': [],
        # 'SINGLE Transport': [],
        # 'SINGLE Climate': [],
        # 'SINGLE Security': [],
        # 'SINGLE TOTAL': [],
        # 'JOINT Health': [],
        # 'JOINT Bioeconomy': [],
        # 'JOINT Energy': [],
        # 'JOINT Transport': [],
        # 'JOINT Climate': [],
        # 'JOINT Security': [],
        # 'TOTAL JOINT': [],

        # col v, w, x

        'Total EU contribution for firms (aggregated per region/year)': [],
        'Total budget of firms (aggregated per region/year)': [],
        'Number of projects with firms exits': [],

        # col al, an
        'Difference to highest Degree Centrality (social challenge patents)': [],
        'Difference to highest Degree Centrality (total patents)': [],



    }

# extracting years

    years = []

    # output_dict["Year"] = Extract_Year_From_Dates(H2020_df, "Project Start Date", output_dict["Year"])

    # output_dict["Year"] = Extract_Year_From_Dates(H2020_df, "Project End Date", output_dict["Year"])
    # output_dict["Year"].sort()

    years = Extract_Year_From_Dates(H2020_df, "Project Start Date", years)

    years = Extract_Year_From_Dates(H2020_df, "Project End Date", years)
    years.sort()
    print(years)


# Changing the Column Formamt to Date-time

    H2020_df['Project Start Date'] = pd.to_datetime(
        H2020_df['Project Start Date'], format="%d/%m/%Y", errors='coerce')
    H2020_df['Project End Date'] = pd.to_datetime(
        H2020_df['Project End Date'], format="%d/%m/%Y", errors='coerce')
    H2020_df['Contract signature date'] = pd.to_datetime(
        H2020_df['Contract signature date'], format="%d/%m/%Y", errors='coerce')


# FOR COLUMN A-I for Geographical fixed charactersitics

    print("Loading NUTS3 file and its sheets...")

    sheet_list = ['All regions', 'Urban-rural', 'Metropolitan', 'Urban-rural remoteness',
                  'Border regions', 'Coastal regions', 'Mountain regions', 'Islands']
    NUTS3_df = Create_DFs_List(NUTS3_file_path, sheet_list)

    print("Loaded!!!!")

    # nut3_code_list = NUTS3_df.get('All regions')['Code 2021'].unique()

    # nut3_code_list = ['DED51', 'ES300', 'ES705', 'FRK24']
    nut3_code_list = ['DED51']

    # print(nut3_code_list)

    df_ALL_REG = NUTS3_df.get('All regions')
    df_URB_RUL_REG = NUTS3_df.get('Urban-rural')
    df_MET_REG = NUTS3_df.get('Metropolitan')
    df_URB_RUL_REMOT_REG = NUTS3_df.get('Urban-rural remoteness')
    df_BORDER_REG = NUTS3_df.get('Border regions')
    df_COSTAL_REG = NUTS3_df.get('Coastal regions')
    df_MOUNT_REG = NUTS3_df.get('Mountain regions')
    df_ISLAND = NUTS3_df.get('Islands')

    for i in nut3_code_list:
        print(i)

        country = ''
        if (i in df_ALL_REG['Code 2021'].unique()):
            country = df_ALL_REG.loc[df_ALL_REG['Code 2021']
                                     == i, 'Country'].iloc[0]
            print(country)

        urb_rul = ''
        if (i in df_URB_RUL_REG['NUTS_ID'].unique()):
            urb_rul = df_URB_RUL_REG.loc[df_URB_RUL_REG['NUTS_ID']
                                         == i, 'Code'].iloc[0]
            print(urb_rul)

        met = ''
        if (i in df_MET_REG['NUTS_ID'].unique()):
            met = df_MET_REG.loc[df_MET_REG['NUTS_ID'] == i, 'Code'].iloc[0]
            print(met)

        urb_rul_remot = ''
        if (i in df_URB_RUL_REMOT_REG['NUTS_ID'].unique()):
            urb_rul_remot = df_URB_RUL_REMOT_REG.loc[df_URB_RUL_REMOT_REG['NUTS_ID']
                                                     == i, 'Code'].iloc[0]
            print(urb_rul_remot)

        border = ''
        if (i in df_BORDER_REG['NUTS_ID'].unique()):
            border = df_BORDER_REG.loc[df_BORDER_REG['NUTS_ID']
                                       == i, 'Code'].iloc[0]
            print(border)

        costal = ''
        if (i in df_COSTAL_REG['NUTS_ID'].unique()):
            costal = df_COSTAL_REG.loc[df_COSTAL_REG['NUTS_ID']
                                       == i, 'Code'].iloc[0]
            print(costal)

        mount = ''
        if (i in df_MOUNT_REG['NUTS_ID'].unique()):
            mount = df_MOUNT_REG.loc[df_MOUNT_REG['NUTS_ID']
                                     == i, 'Code'].iloc[0]
            print(mount)

        island = ''
        if (i in df_ISLAND['NUTS_ID'].unique()):
            island = df_ISLAND.loc[df_ISLAND['NUTS_ID'] == i, 'Code'].iloc[0]
            print(island)

        for y in years:
            output_dict["Country"].append(country)
            output_dict["NUTS3"].append(i)
            output_dict["Urban-rural"].append(urb_rul)
            output_dict["Metropolitan"].append(met)
            output_dict["urban-rural remoteness"].append(urb_rul_remot)
            output_dict["Border region"].append(border)
            output_dict["Coastal region"].append(costal)
            output_dict["Mountain region"].append(mount)
            output_dict["Island"].append(island)
            output_dict["Year"].append(y)

        output_dict = Cals_For_Codes(H2020_df, IPR_df, i, output_dict,
                                     years, Patent_df, Brico_df, Network_total_df, Patents_Social_df)

    print(output_dict)

    main_df = pd.DataFrame()

    for key in output_dict:
        print(key, ": ", " len: ", len(output_dict[key]))

    print('Optimizing file....')
    outputdf = pd.DataFrame(data=output_dict)
    # outputdf.index = outputdf.index+2
    # outputdf = outputdf.reindex(np.arange(len(outputdf) + 2))
    outputdf = outputdf.replace(np.nan, "")

    for column in outputdf:
        main_df[column] = outputdf[column]

    print(main_df)

    print('Saving file...')
    saved_path = Save_Excel_File(
        main_df, H2020_file_path, '(modified).xlsx', '/output/', abs_path)
    print('file saved at ',  saved_path)
    print('Finished!!!!')


def Start_Editing(NUTS3_file_path, H2020_file_path, IPR_file_path, Patents_file_path, Brico_file_path, Network_total_file_path, Patents_Social_file_path):

    print('Started!!!!!')
    abs_path = Path(H2020_file_path).parent
    main(NUTS3_file_path, H2020_file_path, IPR_file_path, abs_path, Patents_file_path,
         Brico_file_path, Network_total_file_path, Patents_Social_file_path)


NUTS3_file_path = "E:\Freelance/vaiolb\Main\Input_output/NUTS3.xlsx"
H2020_file_path = "E:\Freelance/vaiolb\Main\Input_output/02 H2020 NOVEM 22.xlsx"
IPR_file_path = "E:\Freelance/vaiolb\Main\Input_output/IPR(modified).xlsx"
Patents_file_path = "E:\Freelance/vaiolb\Main\Input_output/PATENTS.csv"
Brico_file_path = "E:\Freelance/vaiolb\Main\Input_output/07 BRICO.xlsx"
Network_total_file_path = "E:\Freelance/vaiolb\Main\Input_output/06 NETWORK TOTALS.xlsx"
Patents_Social_file_path = "E:\Freelance/vaiolb\Main\Input_output/05 PATENTS SOCIAL.xlsx"


abs_path = Path(H2020_file_path).parent


main(NUTS3_file_path, H2020_file_path, IPR_file_path, abs_path, Patents_file_path,
     Brico_file_path, Network_total_file_path, Patents_Social_file_path)
