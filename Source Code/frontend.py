from script import Start_Editing
from tkinter import *
from tkinter import ttk
from tkinter import filedialog
import traceback


def Close():
    window.destroy()

def get_NUTS3_file_path():
    nuts3_file_path = filedialog.askopenfilename(initialdir = "/home", title = "Select your NUTS3 file", filetypes = [('Excel', ('*.xls', '*.xlsx'))])
    if nuts3_file_path and ( nuts3_file_path.endswith(".xlsx") or nuts3_file_path.endswith(".xls") ):
        nuts3_file.set(nuts3_file_path)
        nuts3_entry_error_label.config(text="")
    else:
        nuts3_entry_error_label.config(text="select a valid excel file.")


def get_H2020_file_path():
    ref_file_1 = filedialog.askopenfilename(initialdir = "/home", title = "Select your H2020 file", filetypes = [('Excel', ('*.xls', '*.xlsx'))])
    if ref_file_1:
        H2020_folder.set(ref_file_1)
        H2020_entry_error_label.config(text="")
    else:
        H2020_entry_error_label.config(text="select a valid file.")

def get_IPR_folder_path():
    ref_file_2 = filedialog.askopenfilename(initialdir = "/home", title = "Select your first IPR file", filetypes = [('Excel', ('*.xls', '*.xlsx'))])
    if ref_file_2:
        IPR_folder.set(ref_file_2)
        IPR_entry_error_label.config(text="")
    else:
        IPR_entry_error_label.config(text="select a valid file.")

def get_Patents_folder_path():
    ref_file_3 = filedialog.askopenfilename(initialdir = "/home", title = "Select your first Patents file", filetypes = [("CSV Files",("*.csv"))])
    if ref_file_3:
        Patents_folder.set(ref_file_3)
        Patents_entry_error_label.config(text="")
    else:
        Patents_entry_error_label.config(text="select a valid file.")

def start_process():
    try:
        # int(record_entry.get())
        record_error_label.config(text="")
    except ValueError:
        record_error_label.config(text="Something missing a percentage or value.")
        return None
    # percentage = int(record_entry.get())
    nuts3_file_path = nuts3_file.get()
    H2020_file_path = H2020_folder.get()
    IPR_folder_path = IPR_folder.get()
    Patents_folder_path = Patents_folder.get()
    
 

    if not nuts3_file_path:
        nuts3_entry_error_label.config(text="select a valid excel file.")
    elif not H2020_file_path:
        H2020_entry_error_label.config(text="select a valid folder.")
    elif not IPR_folder_path:
        IPR_entry_error_label.config(text="select a valid folder.")
    elif not IPR_folder_path:
        Patents_entry_error_label.config(text="select a valid folder.")
    else:
        Patents_entry_error_label.config(text="")
        IPR_entry_error_label.config(text="")
        H2020_entry_error_label.config(text="")
        nuts3_entry_error_label.config(text="")
        try:
            Start_Editing(nuts3_file_path, H2020_file_path, IPR_folder_path, Patents_folder_path)
            process_info_label.config(text="Success: files generated.")
        except:
            process_error_label.config(text="Error: Check error.txt file")
            with open('error.txt', 'w') as f:
                traceback.print_exc(file=f)


window = Tk()
window.geometry("465x200")
window.title("Excel Convertor")

window.minsize(700, 400)
window.maxsize(700, 400)

nuts3_file= StringVar()
H2020_folder = StringVar()
IPR_folder = StringVar()
Patents_folder = StringVar()
Nyinflyttade_folder = StringVar()
Ny_fil_med_avlidna_folder = StringVar()


heading = Label(window, text="EXCEL FILE EDITOR", font=("Helvetica", 15))
heading.place(x=160, y=10)

################# NUTS3
main_label = Label(window ,text="NUTS3 File")
main_label.place(x=10, y=60)

main_entry = Entry(window, textvariable = nuts3_file, width=60)
main_entry.place(x=100, y=60)

nuts3_entry_error_label = Label(window, text="", fg='red', font=("Helvetica", 10))
nuts3_entry_error_label.place(x=100, y=85)

main_button = ttk.Button(window, text="Browse Folder", command=get_NUTS3_file_path)
main_button.place(x=500, y=60)

################## ref 1
H2020_label = Label(window ,text="H2020 file ")
H2020_label.place(x=10, y=100)

H2020_entry = Entry(window, textvariable = H2020_folder, width=60)
H2020_entry.place(x=100, y=100)

H2020_entry_error_label = Label(window, text="", fg='red', font=("Helvetica", 10))
H2020_entry_error_label.place(x=160, y=125)

H2020_button = ttk.Button(window, text="Browse Folder", command=get_H2020_file_path)
H2020_button.place(x=500, y=100)

################## IPR
IPR_label = Label(window ,text="IPR file ")
IPR_label.place(x=10, y=140)

IPR_entry = Entry(window, textvariable = IPR_folder, width=60)
IPR_entry.place(x=100, y=140)

IPR_entry_error_label = Label(window, text="", fg='red', font=("Helvetica", 10))
IPR_entry_error_label.place(x=160, y=165)

IPR_button = ttk.Button(window, text="Browse Folder", command=get_IPR_folder_path)
IPR_button.place(x=500, y=140)

################## Patents
Patents_label = Label(window ,text="Patents file ")
Patents_label.place(x=10, y=180)

Patents_entry = Entry(window, textvariable = Patents_folder, width=60)
Patents_entry.place(x=100, y=180)

Patents_entry_error_label = Label(window, text="", fg='red', font=("Helvetica", 10))
Patents_entry_error_label.place(x=160, y=205)

Patents_button = ttk.Button(window, text="Browse Folder", command=get_Patents_folder_path)
Patents_button.place(x=500, y=180)


record_error_label = Label(window, text="", fg='red', font=("Helvetica", 10))
record_error_label.place(x=10, y=250)

start_button = ttk.Button(window ,text="Start", width=20, command=start_process)
start_button.place(x=140, y=300)

exit_button = ttk.Button(window, text="Exit", width=20, command=Close)
exit_button.place(x=290, y=300)

process_info_label = Label(window, text="", fg='green', font=("Helvetica", 10))
process_info_label.place(x=162, y=250)

process_error_label = Label(window, text="", fg='red', font=("Helvetica", 10))
process_error_label.place(x=162, y=250)

window.mainloop()