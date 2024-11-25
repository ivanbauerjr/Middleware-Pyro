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
        self.epoch = 1  # Época inicial
        self.offset = 0  # Offset inicial
        self.voters = []
        
    def get_role(self):
        return self.role  # Método para retornar o role do broker

    def update_role(self, new_role):
        self.role = new_role
        print(f"[{self.role} {self.name}] Role atualizado para {new_role}.")

    def update_log(self, leader_data, leader_offset, leader_epoch):
        self.data = leader_data
        self.epoch = leader_epoch
        self.offset = leader_offset
        print(f"[{self.role} {self.name}] Log atualizado: Época: {self.epoch}, Offset: {self.offset}")

    def replicate_log(self):
        self.send_search_request(self.epoch, self.offset)

    #3. Os votantes, ao receberem a notificação, enviam uma requisição
    #de busca ao líder, incluindo a época de onde querem buscar os
    #dados (isto é, época do último offset que possuem em seu log
    #local) e o offset de busca;
    def send_search_request(self, epoch, offset):
        try:
            with Pyro5.api.locate_ns() as ns:
                leader_uri = ns.lookup(f"Lider_Epoca{epoch}")
                leader = Pyro5.api.Proxy(leader_uri)
                leader.handle_search_request(epoch, offset, str(self.uri))
        except Exception as e:
            print(f"[Votante {self.name}] Erro ao enviar requisição de busca: {e}")


    def receive_data(self, data):
        self.data.append(data)  # Adiciona ao log local
        self.offset = len(self.data)
        print(f"[{self.role} {self.name}] Dados recebidos: {data}, Offset: {self.offset}")
        try:
            with Pyro5.api.locate_ns() as ns:
                leader_uri = ns.lookup(f"Lider_Epoca{self.epoch}")
                leader = Pyro5.api.Proxy(leader_uri)
                leader.confirm_commit(self.offset, str(self.uri))
        except Exception as e:
            print(f"[Votante {self.name}] Falha ao confirmar mensagem: {e}")

    # 5. Pode ocorrer de um votante divergir do líder em relação às entradas do log...
    def receive_error(self, epoch, offset):
        print(f"[Votante {self.name}] Erro recebido. A época {epoch} possui dados confirmados até o offset {offset}.")
        # Truncando o log local até o offset informado
        self.data = self.data[:offset]
        self.offset = offset
        # Reenviando a requisição de busca com a época e offset corretos
        self.send_search_request(epoch, offset)
    
    # Não deve ocorrer pois estamos assumindo que o líder é sempre confiável
    def receive_epoch_error(self, epoch):
        print(f"[Votante {self.name}] Erro de época recebido. A época correta é {epoch}.")
        # Atualizando a época e reenviando a requisição de busca
        self.epoch = epoch
        self.send_search_request(epoch, self.offset)

    '''
    def replicate_log(self):
        if leader_epoch == self.epoch:  # Verifica se a época é consistente
            self.data.append(message)  # Adiciona ao log local
            self.offset += 1
            print(f"[Votante {self.name}] Mensagem replicada: {message}, Offset: {self.offset}")
            # Envia confirmação ao líder com o URI do votante
            try:
                with Pyro5.api.locate_ns() as ns:
                    leader_uri = ns.lookup(f"Lider_Epoca{self.epoch}")
                    leader = Pyro5.api.Proxy(leader_uri)
                    leader.confirm_commit(self.offset, str(self.uri))
            except Exception as e:
                print(f"[Votante {self.name}] Falha ao confirmar mensagem: {e}")
        else:
            print(f"[Votante {self.name}] Época inconsistente. Local: {self.epoch}, Líder: {leader_epoch}")
    '''


    # Envia heartbeats periódicos ao líder.
    def send_heartbeat(self, leader_uri):
        leader = Pyro5.api.Proxy(leader_uri)
        while True:
            try:
                # Se for um votante, envia o heartbeat com o URI do votante
                if self.role == "Votante":
                    leader.register_heartbeat(str(self.uri))  # Envia a URI como string
                    print(f"[{self.role} {self.name}] Heartbeat enviado para o líder.")
            except Exception as e:
                print(f"[{self.role} {self.name}] Erro ao enviar heartbeat: {e}")
            time.sleep(9)  # Ajuste o intervalo de envio do heartbeat
            #time.sleep(33)  # Teste para fazer o temporizador falhar


    def promote_to_voter(self):
        print(f"[{self.role} {self.name}] Promovido a votante.")
        self.role = "Votante"
        # O novo votante deverá solicitar dados ao líder para garantir que possui a versão mais recente dos dados.
        with Pyro5.api.locate_ns() as ns:
            leader_uri = ns.lookup("Lider_Epoca1")
            leader = Pyro5.api.Proxy(leader_uri)
            self.update_log(leader.get_data(), leader.get_offset(), leader.get_epoch())

    def update_voter_list(self, voters):
        def update():
            #time.sleep(1)
            self.voters = voters
            print(f"[{self.role} {self.name}] Lista de votantes atualizada: {self.voters}")
        
        # Rodando a atualização da lista em uma thread separada
        Thread(target=update).start()

def start_broker(name, role):
    print(f"Iniciando broker {role} {name}...")  # Print indicando que a thread foi iniciada
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

        # Inicia heartbeat
        Thread(target=broker.send_heartbeat, args=(leader_uri,)).start()

    daemon.requestLoop()
    print(f"Broker {role} {name} terminado.")  # Print indicando que a execução do broker terminou


if __name__ == "__main__":
    print("Iniciando threads para os brokers...")  # Indicação de que as threads estão sendo iniciadas
    Thread(target=start_broker, args=("Votante1", "Votante")).start()
    Thread(target=start_broker, args=("Votante2", "Votante")).start()
    Thread(target=start_broker, args=("Observador1", "Observador")).start()
    print("Threads de brokers iniciadas.")  # Indicação de que todas as threads foram lançadas
