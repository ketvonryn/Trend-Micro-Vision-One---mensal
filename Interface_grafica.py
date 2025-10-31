# Interface_grafica.py
# Janela simples de logs em tempo real para conectar no Tee do seu main.

import threading
import queue
import tkinter as tk
from tkinter.scrolledtext import ScrolledText
from tkinter import ttk


class _GuiStream:
    """Stream compatível com Tee: implementa write/flush e entrega linhas à fila da GUI."""
    def __init__(self, q: queue.Queue):
        self._q = q
        self._buf = []

    def write(self, data: str):
        if not data:
            return
        # Acumula e envia por linha; preserva quebras parciais do Tee/print
        self._buf.append(data)
        joined = "".join(self._buf)
        if "\n" in joined:
            lines = joined.split("\n")
            # mantém o pedaço final (possível linha incompleta) no buffer
            self._buf = [lines.pop()]
            for line in lines:
                if line:  # ignora linhas vazias estritas
                    self._q.put_nowait(line)
        else:
            # sem newline ainda, só acumula
            pass

    def flush(self):
        # Se quiser forçar envio do que sobrou sem newline:
        if self._buf:
            pending = "".join(self._buf).strip()
            if pending:
                self._q.put_nowait(pending)
            self._buf.clear()


class LogViewer:
    """
    Janela com ScrolledText para exibir logs em tempo real.
    Use .stream para plugar no seu Tee: Tee(sys.stdout, log, viewer.stream)
    Chame .start() para abrir a janela sem bloquear seu fluxo.
    """
    def __init__(self, title: str = "Execução em tempo real - Logs", geometry: str = "900x520"):
        self._title = title
        self._geometry = geometry
        self._q = queue.Queue()
        self.stream = _GuiStream(self._q)  # <-- este é o "arquivo" para o Tee
        self._thread = None
        self._root = None
        self._text = None
        self._closing = False

    # ---------- API pública ----------
    def start(self):
        """Abre a janela em uma thread dedicada (não bloqueia o restante do programa)."""
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        """Fecha a janela (opcional)."""
        self._closing = True
        if self._root:
            try:
                self._root.after(0, self._root.destroy)
            except Exception:
                pass

    # ---------- Internos ----------
    def _run(self):
        # Cria UI
        self._root = tk.Tk()
        self._root.title(self._title)
        self._root.geometry(self._geometry)

        # Caixa de texto com rolagem
        self._text = ScrolledText(self._root, wrap="word")
        self._text.pack(fill="both", expand=True, padx=8, pady=8)
        self._text.config(state="disabled")

        # Barra inferior (opcional): botão limpar
        bar = ttk.Frame(self._root)
        bar.pack(fill="x", padx=8, pady=(0, 8))
        ttk.Button(bar, text="Limpar", command=self._clear).pack(side="left")

        # Poll da fila de logs
        self._root.after(100, self._poll_queue)
        self._root.protocol("WM_DELETE_WINDOW", self.stop)

        # Loop da interface
        self._root.mainloop()

    def _clear(self):
        self._text.config(state="normal")
        self._text.delete("1.0", "end")
        self._text.config(state="disabled")

    def _append_line(self, line: str):
        self._text.config(state="normal")
        self._text.insert("end", line + "\n")
        self._text.see("end")
        self._text.config(state="disabled")

    def _poll_queue(self):
        if self._closing:
            return
        try:
            while True:
                line = self._q.get_nowait()
                self._append_line(line)
        except queue.Empty:
            pass
        # agenda próximo polling
        if self._root and not self._closing:
            self._root.after(100, self._poll_queue)
