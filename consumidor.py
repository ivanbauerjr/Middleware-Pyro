import Pyro5.api
import tkinter as tk
from tkinter import messagebox

def atualizar_mensagens():
    try:
        with Pyro5.api.Proxy("PYRONAME:Lider_Epoca1") as lider:
            mensagens = lider.get_data()
            listbox.delete(0, tk.END)  # Limpa a lista
            if mensagens:
                for msg in mensagens:
                    listbox.insert(tk.END, msg)
            else:
                listbox.insert(tk.END, "Nenhuma mensagem disponível.")
    except Exception as e:
        messagebox.showerror("Erro", f"Erro ao buscar mensagens: {e}")

# Criando a janela Tkinter
root = tk.Tk()
root.title("Consumidor")

label = tk.Label(root, text="Mensagens confirmadas:")
label.pack()

listbox = tk.Listbox(root, width=50, height=10)
listbox.pack()

refresh_button = tk.Button(root, text="Atualizar Mensagens", command=atualizar_mensagens)
refresh_button.pack()

# Inicializa a interface com uma primeira atualização
atualizar_mensagens()

root.mainloop()
