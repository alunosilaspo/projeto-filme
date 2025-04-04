import tkinter as tk
from tkinter import scrolledtext





# Função para gerar respostas do chatbot
def chatbot_response(user_input):
    if user_input.lower() == "olá":
        return "Olá! Como posso ajudar você hoje?"
    elif user_input.lower() == "adeus":
        return "Tchau! Tenha um ótimo dia!"
    elif user_input.lower() == "ajuda":
        return "Em que posso ajudar?"
    elif user_input.lower() == "robo":
        return "Sim, sou um robô! Estou aqui para ajudar."
    elif user_input.lower() == "feliz":
        return "Você está feliz?" if user_input.lower() == "sim" return "Fico feliz por você!!" elif return "Sinto muito por você estar triste" 
    elif user_input.lower() == "sair":
        return exit()
    else:
        return f"Você disse: {user_input}!"

# Função para enviar mensagem
def send_message():
    user_message = user_input.get()
    if user_message.strip() != "":
        # Exibir mensagem do usuário
        chat_log.config(state=tk.NORMAL)
        chat_log.insert(tk.END, "Você: " + user_message + "\n")

        # Gerar e exibir resposta do chatbot
        response = chatbot_response(user_message)
        chat_log.insert(tk.END, "Chatbot: " + response + "\n\n")

        chat_log.config(state=tk.DISABLED)
        chat_log.yview(tk.END)  # Rolar para o final do log
        user_input.delete(0, tk.END)
        

# Criar a janela principal
window = tk.Tk()
window.title("Chatbot com Tkinter")
window.geometry("400x500")

# Log de chat (área para exibir mensagens)
chat_log = scrolledtext.ScrolledText(window, state=tk.DISABLED, wrap=tk.WORD)
chat_log.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)

# Entrada de texto para o usuário
user_input = tk.Entry(window, text="Você:", font=("Arial", 12))
user_input.pack(pady=5, padx=10, fill=tk.X)

# Botão de envio
send_button = tk.Button(window, text="Enviar", command=send_message, font=("Arial", 12), bg="lightblue")
send_button.pack(pady=5)

# Rodar o loop principal da interface
window.mainloop()