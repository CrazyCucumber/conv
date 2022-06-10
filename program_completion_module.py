# Здесь описан класс, который завершает работу программы
# Если по истечении timer пользователь не ввел ничего при
# вызове input()
from threading import Thread


class Inp:
    inp = None

    def __init__(self):
        t = Thread(target=self.get_input)
        t.daemon = True
        t.start()
        t.join(timeout=30)

    def get_input(self):
        self.inp = input()

    def get(self):
        return self.inp
