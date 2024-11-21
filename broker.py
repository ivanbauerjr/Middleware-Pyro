import Pyro5.api
from threading import Thread
import time

@Pyro5.api.expose
class Broker:
    def __init__(self, name, role):
        self.name = name
        self.role = role
        self.uri = None
        self.data = []

    def get_role(self):
        return self.role  # Método para retornar o role do broker

    def update_log(self, leader_log):
        self.data = leader_log
        print(f"[{self.role} {self.name}] Log atualizado: {self.data}")

    def replicate_log(self, message):
        self.data.append(message)  # Adiciona ao log local
        print(f"[Votante {self.name}] Mensagem replicada: {message}")
        # Envia confirmação ao líder
        try:
            with Pyro5.api.locate_ns() as ns:
                leader_uri = ns.lookup("Lider_Epoca1")
                leader = Pyro5.api.Proxy(leader_uri)
                leader.confirm_commit(message, self.name)
        except Exception as e:
            print(f"[Votante {self.name}] Falha ao confirmar mensagem: {e}")

    #Envia heartbeats periódicos ao líder.
    def send_heartbeat(self, leader_uri):
        leader = Pyro5.api.Proxy(leader_uri)
        while True:
            try:
                leader.register_heartbeat(str(self.uri))  # Envia a URI como string
                print(f"[{self.role} {self.name}] Heartbeat enviado para o líder.")
            except Exception as e:
                print(f"[{self.role} {self.name}] Erro ao enviar heartbeat: {e}")
            time.sleep(5)  # Ajuste o intervalo de envio do heartbeat
            #time.sleep(11)  # Teste para fazer o temporizador falhar


    def promote_to_voter(self):
        self.role = "Votante"
        print(f"[{self.role} {self.name}] Promovido a votante.")
        #O novo votante deverá solicitar dados ao líder para garantir que possui a versão mais recente dos dados.
        with Pyro5.api.locate_ns() as ns:
            leader_uri = ns.lookup("Lider_Epoca1")
            leader = Pyro5.api.Proxy(leader_uri)
            self.update_log(leader.get_confirmed_messages())

    


def start_broker(name, role):
    broker = Broker(name, role)
    daemon = Pyro5.api.Daemon()
    uri = daemon.register(broker)
    broker.uri = uri  # Atribui a URI ao broker
    
    with Pyro5.api.locate_ns() as ns:
        leader_uri = ns.lookup("Lider_Epoca1")
        leader = Pyro5.api.Proxy(leader_uri)

        # Passa a URI e o papel do broker (votante ou observador)
        leader.register_subscriber(str(uri), role)
        print(f"[{role} {name}] Registrado no líder em {uri}")

        # Inicia heartbeat para brokers votantes
        if role != "Observador":
            Thread(target=broker.send_heartbeat, args=(leader_uri,)).start()

    daemon.requestLoop()



if __name__ == "__main__":
    Thread(target=start_broker, args=("Votante1", "Votante")).start()
    Thread(target=start_broker, args=("Votante2", "Votante")).start()
    Thread(target=start_broker, args=("Observador1", "Observador")).start()