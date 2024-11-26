import threading
import time
import Pyro5.api

@Pyro5.api.expose
class Lider:
    def __init__(self):
        self.data = []
        self.subscribers = {} # Participantes, tanto votantes quanto observadores
        self.last_heartbeat = {}
        self.timeout = 15  # Intervalo de tempo em segundos para checar heartbeats
        self.pending_confirmations = {}
        self.confirmed_messages = []  # Mensagens confirmadas por quórum
        self.quorum_size = 2 #len(self.subscribers) // 2 + 1
        self.epoch = 1
        self.offset = 0

    def get_epoch(self):
        return self.epoch

    def get_confirmed_messages(self):
        return self.confirmed_messages
    
    def get_data(self):
        return self.data
    
    def get_offset(self):
        return self.offset

    # 1. O líder recebe uma gravação e a adiciona ao seu log
    def publish_message(self, message):
        # Incrementa o offset para cada nova mensagem
        self.offset += 1
        log_entry = {"epoch": self.epoch, "offset": self.offset, "message": message}
        self.data.append(log_entry)
        print(f"[Líder] Mensagem adicionada ao log (pendente): {log_entry}")
        self.pending_confirmations[self.offset] = []  # Usar offset como chave

        # 2. O líder notifica os votantes para que busquem os dados atualizados
        for subscriber_uri in self.subscribers:
            try:
                subscriber = Pyro5.api.Proxy(subscriber_uri)
                role = subscriber.get_role()
                if "Votante" in role:
                    subscriber.replicate_log()
            except Exception as e:
                print(f"[Líder] Falha ao enviar mensagem para {subscriber_uri}: {e}")

    #4. Ao receber uma requisição de busca, o líder verifica se o offset de
    #busca e a época de busca são consistentes com seu próprio log.
    #Se não coincidirem, a resposta da busca indicará um erro e
    #informará a maior época e seu offset final antes da época
    #solicitada. Se a solicitação for válida, ele retornará os dados
    #correspondentes a partir do offset fornecido na requisição;
    def handle_search_request(self, epoch, offset, voter_uri):
        if epoch == self.epoch:
            if offset < self.offset:
                # O votante solicitou dados que já existem no log
                data_to_send = self.data[offset:]
                try:
                    # Enviar dados de volta ao votante
                    voter = Pyro5.api.Proxy(voter_uri)
                    voter.receive_data(data_to_send)
                    print(f"[Líder] Dados enviados para {voter_uri}: {data_to_send}")
                except Exception as e:
                    print(f"[Líder] Erro ao enviar dados para {voter_uri}: {e}")
            else:
                print(f"[Líder] Offset solicitado maior do que o disponível. Erro.")
                # Caso o offset solicitado seja maior que o disponível, retorna o erro
                try:
                    voter = Pyro5.api.Proxy(voter_uri)
                    voter.receive_error(self.epoch, self.offset)
                except Exception as e:
                    print(f"[Líder {self.name}] Erro ao responder erro ao {voter_uri}: {e}")
        else:
            print(f"[Líder {self.name}] Requisição de época inconsistente: {epoch} != {self.epoch}")
            try:
                # Se a época do líder for diferente, envia erro
                voter = Pyro5.api.Proxy(voter_uri)
                voter.receive_epoch_error(self.epoch)
            except Exception as e:
                print(f"[Líder {self.name}] Erro ao responder erro de época ao {voter_uri}: {e}")

    def confirm_commit(self, offset, voter_uri):
        if offset not in self.pending_confirmations:
            return  # Offset não está pendente

        # Adiciona o URI do votante à lista de confirmações pendentes
        if voter_uri not in self.pending_confirmations[offset]:
            self.pending_confirmations[offset].append(voter_uri)

        # Verifica se o quórum foi atingido
        if len(self.pending_confirmations[offset]) >= self.quorum_size:
            print(f"[Líder] Mensagem com offset {offset} confirmada e comprometida.")

            # Adicionar a mensagem completa com epoch e offset
            '''
                self.confirmed_messages.append(
                    next(entry for entry in self.data if entry["offset"] == offset)
                )'''
            
            # Adiciona somente a mensagem
            self.confirmed_messages.append(
                next(entry["message"] for entry in self.data if entry["offset"] == offset)
            )
            del self.pending_confirmations[offset]
    

    #Registra um broker no líder e armazena seu estado (votante ou observador).
    def register_subscriber(self, subscriber_uri, role):
        if subscriber_uri not in self.subscribers:
            self.subscribers[subscriber_uri] = role
            print(f"[Líder] Registrado: {subscriber_uri} como {role}")
        # Se mudou o role
        elif self.subscribers[subscriber_uri] != role:
            self.subscribers[subscriber_uri] = role
            print(f"[Líder] Atualizado: {subscriber_uri} para {role}")
        self.notify_voters_participants_list()

    def notify_voters_participants_list(self):
        voters = [uri for uri, role in self.subscribers.items() if "Votante" in role]
        #print(f"[Líder] Lista de  (voters) atual: {voters}")
        # Cria uma nova thread para não bloquear as threads principais
        def notify():
            for subscriber_uri in list(self.subscribers):
                try:
                    subscriber = Pyro5.api.Proxy(subscriber_uri)
                    subscriber.update_voter_list(voters)  # Envia a lista completa de participantes
                    print(f"Notificando votante {subscriber_uri} sobre a nova lista de participantes.")
                except Exception as e:
                    print(f"Erro ao notificar votante {subscriber_uri}: {e}") 
        # Inicia a notificação em uma nova thread
        threading.Thread(target=notify).start()

    # Registra o heartbeat recebido de um votante
    def register_heartbeat(self, voter_uri):
        self.last_heartbeat[voter_uri] = time.time()  # Atualiza o tempo do último heartbeat
        print(f"[Líder] Heartbeat recebido de {voter_uri}")

    # Verifica a disponibilidade dos votantes com base no tempo do último heartbeat.
    def check_voter_availability(self):
        current_time = time.time()
        print("[Líder] Iniciando checagem de heartbeats...")

        # Iterar sobre uma cópia dos itens do dicionário
        for voter_uri, last_time in list(self.last_heartbeat.items()):
            print(f"[Líder] Checando {voter_uri} - Último heartbeat: {last_time}")
            if current_time - last_time > self.timeout:
                print(f"[Líder] Votante {voter_uri} falhou ou está indisponível.")
                if voter_uri in self.subscribers:
                    # Tentar notificar o votante que falhou e alterar seu papel para Observador
                    try:
                        subscriber = Pyro5.api.Proxy(voter_uri)
                        subscriber.update_role("Observador")
                        # Altera o papel de "Votante" para "Observador"
                        self.subscribers[voter_uri] = "Observador"
                        #print(f"[Líder] Votante {voter_uri} alterado para Observador.")
                    except Exception as e:
                        print(f"[Líder] Erro ao notificar {voter_uri}: {e}")
                        # Remover o votante falho da lista de participantes
                        del self.subscribers[voter_uri]
                    
                        
                    # Remover o votante falho da lista de heartbeats
                    del self.last_heartbeat[voter_uri]

                # Verificar quórum após a remoção
                num_voters = len([uri for uri, role in self.subscribers.items() if "Votante" in role])
                if num_voters < self.quorum_size:
                    print("[Líder] Quórum não atingido. Promovendo observador!")
                    self.promote_observer()
            else:
                print(f"[Líder] Votante {voter_uri} está ativo.")

    def promote_observer(self):
        for subscriber_uri in self.subscribers:
            try:
                subscriber = Pyro5.api.Proxy(subscriber_uri)
                if subscriber.get_role() == "Observador":
                    self.subscribers[subscriber_uri] = "Votante"
                    subscriber.promote_to_voter()
                    print(f"[Líder] Promovido de Observador para Votante: {subscriber_uri} ")
                    #self.notify_voters_participants_list()  # Notificar todos os votantes após a promoção
                    #break  # Assim que promover o primeiro, notifica e sai
            except Exception as e:
                print(f"[Líder] Erro ao alterar de Observador para Votante: {e}")


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