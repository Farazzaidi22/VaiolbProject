from asyncio.windows_events import NULL
from calendar import month
from cmath import nan
from itertools import count
import os
from pathlib import Path
from importlib.resources import path
from re import S
from tokenize import String
from turtle import st
from weakref import ref
import pandas as pd
import numpy as np
import datetime
from dateutil import relativedelta


def Create_DF(file_path):
    print('Creating data frame of file ', file_path , '...')
    df = pd.read_excel(file_path)
    # print(df['Bearbetning: Start'])
    return df

def Create_DFs_List(file_path, sheet_list):
    dict_df = pd.read_excel( file_path, sheet_name=sheet_list )
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

    df.to_excel(file_path_to_be_saved_at, index = 0)
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

    H2020_removed_dups_df['Project Start Date'] = pd.to_datetime(H2020_removed_dups_df['Project Start Date'], format="%d/%m/%Y", errors='coerce')

    ##########################################################################################################################################################

    IPR_df = IPR_df.sort_values('project ID')

    # IPR_df['application date'] = IPR_df['application date'].fillna(pd.Timestamp('2022-12-25'))
    IPR_df['application date'] = pd.to_datetime(IPR_df['application date'], format="%Y-%m-%d", errors='coerce')
    
    # IPR_df['priority date'] = IPR_df['priority date'].fillna(pd.Timestamp('2022-12-25'))
    IPR_df['priority date'] = pd.to_datetime(IPR_df['priority date'], format="%Y-%m-%d", errors='coerce')


    for IPR_index, IPR_row in IPR_df.iterrows():

        project_id = IPR_row['project ID']
        print("For ", project_id)
        
        if not pd.isnull(project_id):
            
            IPR_type = IPR_row['IPR']

            application_date = IPR_row['application date']

            priority_date = IPR_row['priority date']


            if (not pd.isnull(application_date) ) or (not pd.isnull(priority_date)):

                H2020_df_Filtered= H2020_removed_dups_df.loc[H2020_removed_dups_df['Project ID'] == project_id]
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

                            if (not pd.isnull(application_date) ) and (not pd.isnull(priority_date)):
                                print('Finding the smaller date...')

                                smaller_date = Compare_Dates(application_date, priority_date)
                                print("Found!!!")
                                print(smaller_date, " is smaller date")

                                r = relativedelta.relativedelta(smaller_date, start_date)

                                months_difference = (r.years * 12) + r.months
                                print(months_difference, " is month difference")

                                if (months_difference > 12) and (IPR_type == "BACKGROUND"):
                                    print( "For ", IPR_type, " months difference b/w start and application/priority is ", months_difference)
                                    IPR_df.at[IPR_index, 'IPR'] = "FOREGROUND"
                                    print( "Changed to ", IPR_df.at[IPR_index, 'IPR'], "!!!" )

                                elif (months_difference < 12) and (IPR_type == "FOREGROUND"):
                                    print( "For ", IPR_type, " months difference b/w start and application/priority is ", months_difference)
                                    IPR_df.at[IPR_index, 'IPR'] = "BACKGROUND"
                                    print( "Changed to ", IPR_df.at[IPR_index, 'IPR'], "!!!" )

                            
                            elif (not pd.isnull(application_date) ) and (pd.isnull(priority_date)):
                                print('Finding the smaller date...')

                                print("Found!!!")
                                print(application_date, " is smaller date")

                                r = relativedelta.relativedelta(application_date, start_date)

                                months_difference = (r.years * 12) + r.months
                                print(months_difference, " is month difference")

                                if (months_difference > 12) and (IPR_type == "BACKGROUND"):
                                    print( "For ", IPR_type, " months difference b/w start and application/priority is ", months_difference)
                                    IPR_df.at[IPR_index, 'IPR'] = "FOREGROUND"
                                    print( "Changed to ", IPR_df.at[IPR_index, 'IPR'], "!!!" )

                                elif (months_difference < 12) and (IPR_type == "FOREGROUND"):
                                    print( "For ", IPR_type, " months difference b/w start and application/priority is ", months_difference)
                                    IPR_df.at[IPR_index, 'IPR'] = "BACKGROUND"
                                    print( "Changed to ", IPR_df.at[IPR_index, 'IPR'], "!!!" )


                            elif (pd.isnull(application_date)) and (not pd.isnull(priority_date)):
                                print('Finding the smaller date...')

                                print("Found!!!")
                                print(priority_date, " is smaller date")

                                r = relativedelta.relativedelta(priority_date, start_date)

                                months_difference = (r.years * 12) + r.months
                                print(months_difference, " is month difference")

                                if (months_difference > 12) and (IPR_type == "BACKGROUND"):
                                    print( "For ", IPR_type, " months difference b/w start and application/priority is ", months_difference)
                                    IPR_df.at[IPR_index, 'IPR'] = "FOREGROUND"
                                    print( "Changed to ", IPR_df.at[IPR_index, 'IPR'], "!!!" )

                                elif (months_difference < 12) and (IPR_type == "FOREGROUND"):
                                    print( "For ", IPR_type, " months difference b/w start and application/priority is ", months_difference)
                                    IPR_df.at[IPR_index, 'IPR'] = "BACKGROUND"
                                    print( "Changed to ", IPR_df.at[IPR_index, 'IPR'], "!!!" )

                            else:
                                continue


    IPR_df['application date'] = IPR_df['application date'].astype(str)
    IPR_df['priority date'] = IPR_df['priority date'].astype(str)

    saved_path = Save_Excel_File(IPR_df, IPR_file_path, '(modified).xlsx', '/output/', abs_path)
    print('file saved at ',  saved_path)
    
    return IPR_df




def Cals_For_Codes(H2020_df: pd.DataFrame, IPR_df: pd.DataFrame, code , output_dict, year):
    ### 1 - Looking for code in column K 'NUTS 3 Code'
        H2020_df_DED51_pub = H2020_df.loc[H2020_df['NUTS 3 Code'] == code]
    # print(H2020_df_DED51_pub)


    ### 2 - Filtering only Public body from column H 'Legal Entity Type'
        H2020_df_DED51_pub = H2020_df_DED51_pub.loc[H2020_df_DED51_pub["Legal Entity Type"] == "PUB"]
        # print(H2020_df_DED51_pub["Project Start Date"])

    ### 3 - Going to columns T and U for checking the projects life times.

        H2020_df_DED51_pub['Project Start Date'] = pd.to_datetime(H2020_df_DED51_pub['Project Start Date'], format="%d/%m/%Y", errors='coerce')
        H2020_df_DED51_pub['Project End Date'] = pd.to_datetime(H2020_df_DED51_pub['Project End Date'], format="%d/%m/%Y", errors='coerce')


        output_dict["Total number of  public bodies participations, running for at least one year"] += For_Col_K(year, H2020_df_DED51_pub)

    ######### FOR COLUMN L for Total number of unique projects involving public bodies, running for at least one year

        output_dict["Total number of unique  projects involving public bodies, running for at least one year"] +=  For_Col_L(year, H2020_df_DED51_pub)

    ######### FOR COLUMN M for Number project months acummulating  since 2014

        output_dict["Number project months acummulating  since 2014"] +=  For_Col_M(year, H2020_df_DED51_pub)

    ######### FOR COLUMN N for Multiannual EU funds commitment benefitting public bodies aggregated at regional level (date of contract signature  counts)
        
        H2020_df_DED51_pub['Contract signature date'] = pd.to_datetime(H2020_df_DED51_pub['Contract signature date'], format="%d/%m/%Y", errors='coerce')

        output_dict["Multiannual EU funds commitment benefitting   public bodies aggregated at regional level (date of contract signature  counts)"] +=  For_Col_N(year, H2020_df_DED51_pub)


    ######### FOR COLUMN O for Multiannual self financing  commitment benefitting   public bodies aggregated at regional level (date of contract signature  counts)
        
        output_dict["Multiannual self financing  commitment benefitting   public bodies aggregated at regional level (date of contract signature  counts)"] +=  For_Col_O(year, H2020_df_DED51_pub)


    ######### FOR COLUMN P Total number of background IPR from another region accessed via a project that has been running 1 year (counted once per project lifetime)

        output_dict["Total number of background IPR from another region accessed via a project that has been running 1 year (counted once per project lifetime)"] +=  For_Col_P_and_Q(year, IPR_df, 'BACKGROUND', H2020_df, H2020_df_DED51_pub, code)

    ######### FOR COLUMN Q Total number of foreground IPR from another region accessed via a project that has been running 1 year (counted once per project lifetime)

        output_dict["Total number of foreground IPR from another region accessed via a project that has been running 1 year (counted once per project lifetime)"] +=  For_Col_P_and_Q(year, IPR_df, 'FOREGROUND', H2020_df, H2020_df_DED51_pub, code)

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

        df1 = df[ (df['Project Start Date'].dt.strftime('%d-%m-%Y') == '01-01-' + str(year)) ]

        df2 = df[ (df['Project Start Date'].dt.strftime('%Y') < str(year)) & (df['Project End Date'].dt.strftime('%Y') >= str(year)) ]

        result_df = Conact_DFs(df1, df2)
        row_count.append(len(result_df.index))
    
    return row_count

def For_Col_L(year_arr, df):
    row_count = []
    for year in year_arr:
        print("For ", year)

        df1 = df[ (df['Project Start Date'].dt.strftime('%d-%m-%Y') == '01-01-' + str(year)) ]

        df2 = df[ (df['Project Start Date'].dt.strftime('%Y') < str(year)) & (df['Project End Date'].dt.strftime('%Y') >= str(year)) ]

        result_df = Conact_DFs(df1, df2)
        result_df = result_df.drop_duplicates(subset=['Project ID'])
        # print(len(result_df.index), "faraz")
        row_count.append(len(result_df.index))
    
    return row_count

def For_Col_M(year_arr, df):
    unique_months = []
    for year in year_arr:
        print("For ", year)

        df2 = df[ (df['Project Start Date'].dt.strftime('%Y') <= str(year)) ]
        prev_df = df2
        df2 = Conact_DFs(prev_df, df2)
        df2 = df2.drop_duplicates(subset=['Project ID'], keep='first')
        print(df2)

        no_of_months = 0

        for index, row in df2.iterrows():

            print(row['Project Start Date'])
            print(row['Project End Date'])

            if pd.to_datetime(row['Project End Date']).year > int(year):
                no_of_months += ( datetime.datetime(year, 12, 31) - row['Project Start Date'] ) / np.timedelta64(1, 'M')
                print(round(no_of_months))
                no_of_months = round(no_of_months)
            
            
            elif pd.to_datetime(row['Project End Date']).year <= int(year):
                no_of_months += ( row['Project End Date'] - row['Project Start Date'] ) / np.timedelta64(1, 'M')
                print(round(no_of_months))
                no_of_months = round(no_of_months)
        
    
        unique_months.append(no_of_months)
    
    print(unique_months)
    
    return unique_months


def For_Col_N(year_arr, df):
    eu_contribution_count = []
    for year in year_arr:
        print("For ", year)

        df1 = df[ (df['Contract signature date'].dt.strftime('%Y') == str(year)) ]
        print(df1)

        total = df1['EU Contribution (€)'].apply(lambda x: pd.to_numeric(x, errors='coerce')).sum()


        print(total)
       
        eu_contribution_count.append(total)
    
    return eu_contribution_count

def For_Col_O(year_arr, df):
    eu_contribution_count = []
    for year in year_arr:
        print("For ", year)

        df1 = df[ (df['Contract signature date'].dt.strftime('%Y') == str(year)) ]
        print(df1)

        total_eu = df1['EU Contribution (€)'].apply(lambda x: pd.to_numeric(x, errors='coerce')).sum()
        total_tc = df1['H2020 Total Cost'].apply(lambda x: pd.to_numeric(x, errors='coerce')).sum()

        total = total_tc - total_eu

        print(total)
       
        eu_contribution_count.append(total)
    
    return eu_contribution_count


def For_Col_P_and_Q(year_arr, IPR_df, IPR_Type,  H2020_df, H2020_df_DED51_pub, code):

    patt_count_array = [0,0,0,0,0,0,0,0,0]

    H2020_df_DED51_pub = H2020_df_DED51_pub.sort_values('Project ID')
    H2020_pub_Uid_df = H2020_df_DED51_pub.drop_duplicates('Project ID', keep='first')
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
                IPR_df_Filtered.drop_duplicates( subset=['patentFamilyIdentifier'], keep='last', inplace=True)
                print(IPR_df_Filtered)

                if not IPR_df_Filtered.empty:
                    for IPR_df_index, IPR_df_row in IPR_df_Filtered.iterrows():

                        H2020_Org_Id_Filtered_df = H2020_df.loc[ (H2020_df["Project ID"] == IPR_df_row["project ID"]) & (H2020_df["Organisation ID"] == IPR_df_row["Organisation ID"]) ]

                        if not H2020_Org_Id_Filtered_df.empty:
                            print(H2020_Org_Id_Filtered_df)
                            
                            for H2020_Org_Id_Filtered_df_index, row_1 in H2020_Org_Id_Filtered_df.iterrows():
                            
                                if row_1['NUTS 3 Code'] != code:

                                    project_start_year = datetime.datetime.strptime(row_1['Project Start Date'], '%d/%m/%Y').year
                                    print(project_start_year)

                                    if project_start_year in year_arr:
                                        index = year_arr.index(project_start_year)
                                        patt_count_array[index + 1] = patt_count_array[index + 1] + 1
                                        print(patt_count_array[index + 1])


    return patt_count_array

def main(NUTS3_file_path, H2020_file_path, IPR_file_path, abs_path): #NUTS3_file_path, IPR_file_path, abs_path):

######### FOR COLUMN K for Total number of public bodies participations, running for at least one year

    # Opening H2020.xlsx 
    H2020_df = Create_DF(H2020_file_path)

    # Opening IPR.xlsx
    IPR_df = Create_DF(IPR_file_path)

    IPR_df = Clean_IPR_Df_Data(IPR_df, H2020_df, IPR_file_path, abs_path)


### columns
    output_dict = { 'Country': [], 'NUTS3': [], 'Urban-rural': [], 'Metropolitan': [], 'urban-rural remoteness': [], 'Border region': [], 'Coastal region': [], 'Mountain region': [], 'Island': [], 'Year': [] , 
    'Total number of  public bodies participations, running for at least one year': [], 
    'Total number of unique  projects involving public bodies, running for at least one year': [], 
    'Number project months acummulating  since 2014': [], 
    'Multiannual EU funds commitment benefitting   public bodies aggregated at regional level (date of contract signature  counts)': [], 
    'Multiannual self financing  commitment benefitting   public bodies aggregated at regional level (date of contract signature  counts)': [],
    'Total number of background IPR from another region accessed via a project that has been running 1 year (counted once per project lifetime)': [], 
    'Total number of foreground IPR from another region accessed via a project that has been running 1 year (counted once per project lifetime)': [] }

### extracting years

    years = []

    # output_dict["Year"] = Extract_Year_From_Dates(H2020_df, "Project Start Date", output_dict["Year"])

    # output_dict["Year"] = Extract_Year_From_Dates(H2020_df, "Project End Date", output_dict["Year"])
    # output_dict["Year"].sort()

    years = Extract_Year_From_Dates(H2020_df, "Project Start Date", years)

    years = Extract_Year_From_Dates(H2020_df, "Project End Date", years)
    years.sort()



######## FOR COLUMN A-I for Geographical fixed charactersitics

    print("Loading NUTS3 file and its sheets...")

    sheet_list = ['All regions', 'Urban-rural', 'Metropolitan', 'Urban-rural remoteness', 'Border regions', 'Coastal regions', 'Mountain regions', 'Islands']
    NUTS3_df = Create_DFs_List(NUTS3_file_path, sheet_list)
    
    print("Loaded!!!!")

    nut3_code_list = NUTS3_df.get('All regions')['Code 2021'].unique()

    # nut3_code_list = ['DED51', 'ES300', 'ES705']
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
            country = df_ALL_REG.loc[df_ALL_REG['Code 2021'] == i, 'Country'].iloc[0]
            print(country)

        urb_rul = ''
        if (i in df_URB_RUL_REG['NUTS_ID'].unique()):
            urb_rul = df_URB_RUL_REG.loc[df_URB_RUL_REG['NUTS_ID'] == i, 'Code'].iloc[0]
            print(urb_rul)

        met = ''
        if (i in df_MET_REG['NUTS_ID'].unique()):
            met = df_MET_REG.loc[df_MET_REG['NUTS_ID'] == i, 'Code'].iloc[0]
            print(met)

        urb_rul_remot = ''
        if (i in df_URB_RUL_REMOT_REG['NUTS_ID'].unique()):
            urb_rul_remot = df_URB_RUL_REMOT_REG.loc[df_URB_RUL_REMOT_REG['NUTS_ID'] == i, 'Code'].iloc[0]
            print(urb_rul_remot)

        border = ''
        if (i in df_BORDER_REG['NUTS_ID'].unique()):
            border = df_BORDER_REG.loc[df_BORDER_REG['NUTS_ID'] == i, 'Code'].iloc[0]
            print(border)

        costal = ''
        if (i in df_COSTAL_REG['NUTS_ID'].unique()):
            costal = df_COSTAL_REG.loc[df_COSTAL_REG['NUTS_ID'] == i, 'Code'].iloc[0]
            print(costal)

        mount = ''
        if (i in df_MOUNT_REG['NUTS_ID'].unique()):
            mount = df_MOUNT_REG.loc[df_MOUNT_REG['NUTS_ID'] == i, 'Code'].iloc[0]
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


        output_dict = Cals_For_Codes(H2020_df, IPR_df, i, output_dict, years)


    print(output_dict)


    main_df = pd.DataFrame()

    print('Optimizing file....')
    outputdf = pd.DataFrame(data=output_dict)
    # outputdf.index = outputdf.index+2
    # outputdf = outputdf.reindex(np.arange(len(outputdf) + 2))
    outputdf = outputdf.replace(np.nan,"")

    for column in outputdf:
        main_df[column] = outputdf[column]
    
    
    print(main_df)

    print('Saving file...')
    saved_path = Save_Excel_File(main_df, H2020_file_path, '(modified).xlsx', '/output/', abs_path)
    print('file saved at ',  saved_path)
    print('Finished!!!!')


def Start_Editing(NUTS3_file_path, H2020_file_path, IPR_file_path):
    
    print('Started!!!!!')
    abs_path = Path(H2020_file_path).parent
    main(NUTS3_file_path, H2020_file_path, IPR_file_path, abs_path)




# NUTS3_file_path = "E:\Freelance/vaiolb\Main\Input_output/NUTS3.xlsx"
# H2020_file_path ="E:\Freelance/vaiolb\Main\Input_output/H2020.xlsx"
# IPR_file_path = "E:\Freelance/vaiolb\Main\Input_output/IPR.xlsx"

# abs_path = Path(H2020_file_path).parent


# main(NUTS3_file_path, H2020_file_path, IPR_file_path, abs_path) 