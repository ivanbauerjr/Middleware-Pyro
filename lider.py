import threading
import time
import Pyro5.api

@Pyro5.api.expose
class Lider:
    def __init__(self):
        self.data = []
        self.subscribers = {}
        self.last_heartbeat = {}
        self.timeout = 7  # Intervalo de tempo em segundos para checar heartbeats
        self.pending_confirmations = {}
        self.confirmed_messages = []  # Mensagens confirmadas por quórum
        self.quorum_size = 2 #len(self.subscribers) // 2 + 1

    def get_confirmed_messages(self):
        return self.confirmed_messages

    def publish_message(self, message):
        self.data.append(message)
        print(f"[Líder] Mensagem adicionada ao log (pendente): {message}")
        self.pending_confirmations[message] = []  # Armazena confirmações pendentes

        # Envia a mensagem para os votantes replicarem
        for subscriber_uri in self.subscribers:
            if "Votante" in self.get_subscriber_role(subscriber_uri):
                try:
                    subscriber = Pyro5.api.Proxy(subscriber_uri)
                    subscriber.replicate_log(message)
                except Exception as e:
                    print(f"[Líder] Falha ao enviar mensagem para {subscriber_uri}: {e}")

    def confirm_commit(self, message, voter_name):
        if message not in self.pending_confirmations:
            return  # Mensagem não está pendente

        if voter_name not in self.pending_confirmations[message]:
            self.pending_confirmations[message].append(voter_name)

        # Verifica se o quórum foi atingido
        if len(self.pending_confirmations[message]) >= self.quorum_size:
            print(f"[Líder] Mensagem {message} confirmada e comprometida.")
            self.confirmed_messages.append(message)
            del self.pending_confirmations[message]

    #Registra um broker no líder e armazena seu estado (votante ou observador).
    def register_subscriber(self, subscriber_uri, role):
        if subscriber_uri not in self.subscribers:
            self.subscribers[subscriber_uri] = role
            print(f"[Líder] Registrado: {subscriber_uri} como {role}")

    # Registra o heartbeat recebido de um votante
    def register_heartbeat(self, voter_uri):
        self.last_heartbeat[voter_uri] = time.time()  # Atualiza o tempo do último heartbeat
        print(f"[Líder] Heartbeat recebido de {voter_uri}")

    def promote_observer(self):
        for subscriber_uri in self.subscribers:
            try:
                subscriber = Pyro5.api.Proxy(subscriber_uri)
                if subscriber.get_role() == "Observador":
                    subscriber.promote_to_voter()
                    return
            except Exception as e:
                print(f"[Líder] Erro ao promover observador: {e}")

    # Verifica a disponibilidade dos votantes com base no tempo do último heartbeat.
    def check_voter_availability(self):
        current_time = time.time()
        print("[Líder] Iniciando checagem de heartbeats...")

        for voter_uri, last_time in self.last_heartbeat.items():
            print(f"[Líder] Checando {voter_uri} - Último heartbeat: {last_time}")
            if current_time - last_time > self.timeout:
                print(f"[Líder] Votante {voter_uri} falhou ou está indisponível.")
                #if len(self.active_voters) < self.quorum_size:
                print("[Líder] Quórum não atingido. Promovendo observador!")
                self.promote_observer()
            else:
                print(f"[Líder] Votante {voter_uri} está ativo.")




    def start_heartbeat_check(self):
            while True:
                self.check_voter_availability()
                time.sleep(self.timeout)  # Aguarda o intervalo de tempo antes de checar novamente


def start_leader():
    print("[Líder] Iniciando o processo do líder...")
    lider = Lider()

    daemon = Pyro5.api.Daemon()
    uri = daemon.register(lider)
    with Pyro5.api.locate_ns() as ns:
        ns.register("Lider_Epoca1", uri)

    print(f"Líder ativo em {uri}")
    
    # Inicia uma thread para o requestLoop do Pyro5
    daemon_thread = threading.Thread(target=daemon.requestLoop)
    daemon_thread.start()

    # Inicia a checagem de heartbeats em uma thread separada
    heartbeat_thread = threading.Thread(target=lider.start_heartbeat_check)
    heartbeat_thread.start()

if __name__ == "__main__":
    start_leader()
