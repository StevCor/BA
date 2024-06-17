from tkinter import *

# Funktion, die ein Pop-up über dem übergebenen tkinter-Container anzeigt
# source: https://thepythoncode.com/article/create-a-simple-file-explorer-using-tkinter-in-python
def popup(root, title, message):
    top = Toplevel(root)
    top.geometry('250x150')
    top.resizable(False, False)
    top.title(title)
    top.columnconfigure(0, weight = 1)
    Label(top, text=message, pady = 10).grid()
    Button(top, text='OK', command = lambda: top.destroy()).grid(pady = 10, sticky ='NSEW')

