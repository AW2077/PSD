import streamlit as st
import pandas as pd
import json
import matplotlib.pyplot as plt
import time
from kafka import KafkaConsumer

# konfiguracja strony
st.set_page_config(page_title="Detekcja Anomalii", page_icon="🚨", layout="wide")
st.title("🚨 Monitor Strumieniowy Anomalii")
st.markdown("Automatyczny podgląd pełnej historii alertów z klastra Apache Flink.")

# pamiec historii alertow
if 'alarms_history' not in st.session_state:
    st.session_state['alarms_history'] = []

# konsument kafki
@st.cache_resource
def get_kafka_consumer():
    return KafkaConsumer(
        'alarms',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=500
    )

consumer = get_kafka_consumer()

# pobieranie wiadomosci z kafki
for msg in consumer:
    st.session_state['alarms_history'].append(msg.value)

data = st.session_state['alarms_history']

# wyciaganie szczegolow anomalii
def parse_anomaly_details(row):
    details = []
    reasons = row.get('anomaly_reasons', [])
    
    if 'LIMIT_EXCEEDED_ANOMALY' in reasons:
        kwota = row.get('amount', 0)
        limit = row.get('available_limit', 0)
        roznica = kwota - limit
        details.append(f"💳 Przekroczono limit o {roznica:.2f} PLN (Transakcja: {kwota} / Limit: {limit})")
        
    if 'HIGH_FREQUENCY_ANOMALY' in reasons:
        sekundy = row.get('time_since_last', 0)
        details.append(f"⏱️ Podejrzana częstotliwość: zaledwie {sekundy} sek. od poprzedniej transakcji")
        
    if 'LOCATION_JUMP_ANOMALY' in reasons:
        z_miasta = row.get('prev_city', 'Nieznane')
        do_miasta = row.get('city', 'Nieznane')
        details.append(f"📍 Niemożliwy skok geograficzny: z [{z_miasta}] do [{do_miasta}]")
        
    if 'STATISTICAL_AMOUNT_ANOMALY' in reasons:
        details.append("📊 Wydatek nietypowy: kwota ponad 3-krotnie wyższa od średniej historii tej karty")
        
    return " | ".join(details) if details else "Brak dodatkowych szczegółów"

# interfejs
if not data:
    st.info("oczekuje na uruchomienie strumienia transakcji...")
else:
    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset=['timestamp', 'card_id'])
    
    # przygotowanie danych do tabeli na gorze
    df['Szczegóły anomalii'] = df.apply(parse_anomaly_details, axis=1)
    df_sorted = df.sort_values(by='timestamp', ascending=False)
    
    # glowna tabela na gorze
    st.markdown("### Najnowsze alerty")
    columns_to_show = ['timestamp', 'card_id', 'amount', 'anomaly_reasons', 'Szczegóły anomalii']
    available_cols = [c for c in columns_to_show if c in df_sorted.columns]
    
    st.dataframe(
        df_sorted[available_cols],
        use_container_width=True,
        hide_index=False,
        height=300,
        column_config={
            "Szczegóły anomalii": st.column_config.TextColumn(
                "Szczegóły anomalii",
                width="large",
            ),
            "anomaly_reasons": st.column_config.ListColumn(
                "Typy anomalii"
            ),
            "amount": st.column_config.NumberColumn(
                "Kwota (PLN)",
                format="%.2f"
            )
        }
    )

    st.divider()

    # statystyki kpi
    st.subheader("Wskaźniki Efektywności")
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    kpi1.metric(label="Wykryte Anomalie", value=len(df))
    kpi2.metric(label="Ochroniony Kapitał", value=f"{df['amount'].sum():.2f} PLN")
    kpi3.metric(label="Zablokowane Karty", value=df['card_id'].nunique())
    
    srednia = df['amount'].mean() if not df.empty else 0
    kpi4.metric(label="Średnia Kwota Fraudu", value=f"{srednia:.2f} PLN")

    st.divider()

    # sekcja glownych wykresow
    chart_col1, chart_col2 = st.columns(2)
    with chart_col1:
        st.markdown("### Profil Wykrywanych Zagrożeń")
        if 'anomaly_reasons' in df.columns:
            reasons = df.explode('anomaly_reasons')
            reason_counts = reasons['anomaly_reasons'].value_counts()
            fig, ax = plt.subplots(figsize=(8, 3.5))
            reason_counts.plot(kind='bar', color='#ff4b4b', ax=ax)
            ax.set_ylabel("Liczba incydentów")
            plt.xticks(rotation=15, ha='right')
            st.pyplot(fig)

    with chart_col2:
        st.markdown("### Miasta z największą liczbą oszustw")
        if 'city' in df.columns:
            city_counts = df['city'].value_counts().head(5)
            fig2, ax2 = plt.subplots(figsize=(8, 3.5))
            city_counts.plot(kind='pie', autopct='%1.1f%%', colormap='Set3', ax=ax2)
            ax2.set_ylabel("")
            st.pyplot(fig2)
            
    st.divider()
    
    # sekcja dodatkowych statystyk
    stat_col1, stat_col2 = st.columns(2)
    with stat_col1:
        st.markdown("### TOP 5 podejrzanych kart")
        top_cards = df['card_id'].value_counts().head(5).reset_index()
        top_cards.columns = ['ID Karty', 'Liczba oszustw']
        st.dataframe(top_cards, use_container_width=True, hide_index=True)
        
    with stat_col2:
        st.markdown("### Dynamika oszustw w czasie")
        df['czas'] = pd.to_datetime(df['timestamp'])
        timeline = df.groupby(df['czas'].dt.floor('S')).size()
        st.line_chart(timeline, use_container_width=True)

# automatyczne odswiezanie
time.sleep(1)
st.rerun()