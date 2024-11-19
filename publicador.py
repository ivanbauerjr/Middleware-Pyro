import Pyro5.api
import tkinter as tk
from tkinter import messagebox

def enviar_mensagem():
    msg = input_field.get()
    if not msg:
        messagebox.showerror("Erro", "Mensagem vazia!")
        return
    
    try:
        with Pyro5.api.Proxy("PYRONAME:Lider-Epoca1") as lider:
            lider.publicar_mensagem(msg)
            messagebox.showinfo("Sucesso", "Mensagem publicada com sucesso!")
            input_field.delete(0, tk.END)
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao publicar: {e}")

# Criando a janela Tkinter
root = tk.Tk()
root.title("Publicador")

label = tk.Label(root, text="Digite sua mensagem:")
label.pack()

input_field = tk.Entry(root, width=50)
input_field.pack()

send_button = tk.Button(root, text="Publicar", command=enviar_mensagem)
send_button.pack()

root.mainloop()