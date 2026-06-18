# System Detekcji Anomalii Finansowych (Real-Time Fraud Detection)

Projekt zrealizowany w ramach przedmiotu Przetwarzanie Strumieni Danych i Data Science. System służy do analizy transakcji płatniczych w czasie rzeczywistym i wykrywania nadużyć finansowych przy użyciu Apache Flink, Kafki oraz MongoDB.

## Architektura
System opiera się na architekturze strumieniowej:
- **Infrastruktura:** Kafka, Zookeeper, MongoDB (uruchamiane przez `docker-compose`).
- **Przetwarzanie (Hot Path):** Apache Flink (PyFlink) – stanowe przetwarzanie danych (Stateful Stream Processing).

## Wymagania systemowe
Aby uruchomić projekt, upewnij się, że masz zainstalowane:
- **Java (JDK 11):** Wymagane dla silnika Apache Flink oraz brokera Apache Kafka.
- **Docker & Docker Compose:** 
- **Python 3.11.9**

## Struktura projektu
- `simulator.py`: Generuje strumień transakcji (10 000 kart, 15 miast). Wprowadza 5% anomalii.
- `detector.py`: Silnik Flinka realizujący detekcję: limitów, statystycznych anomalii kwotowych, burstów i skoków lokalizacji.
- `alarms_consumer.py`: Zapisuje alarmy z Kafki do MongoDB i loguje zdarzenia.
- `test_consumer.py`: Sniffer diagnostyczny do walidacji surowych danych w Kafce.


## Uruchomienie (bash w folderze projektu)
- docker-compose up -d
- python -3.11 -m venv .venv
- source ./.venv/bin/activate (Linux) lub .\.venv\Scripts\activate (Windows)
- pip install -r requirements.txt

## Kolejność uruchomienia
Zalecane uruchomienie w osobnych terminalach:

1. python detector.py

2. python alarms_consumer.py

3. python simulator.py

4. python test_consumer.py (diagnostyka)
