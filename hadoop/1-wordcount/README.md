
# Materiale

* `mapper.py`: script python (da completare) che legge dallo standard input righe di un testo, spezza le righe in parole ed emette in standard output `parola 1`
* `reducer.py`: script python (da completare) che legge dallo standard input stringhe nel formato `parola 1` ed emette in standard output `parola n` dove `parola` e' una parola ricevuta nello standard input e `n` e' il numero di volte che quella parola e' stata ricevuta
* `datasets/mid.txt`: "a midsummer night's dream" book
* `datasets/ulysses.txt`: "Ulysses" - book 
* `solution/`: soluzione completa (`mapper.py` e `reducer.py`)

# Esecuzione

* esecuzione locale senza Hadoop:

`make local`

* esecuzione locale con Hadoop

`make clean`

`make input`

`make hadoop`
