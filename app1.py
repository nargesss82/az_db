import streamlit as st
import pandas as pd
import pyodbc
from datetime import datetime

# App configuration
st.set_page_config(page_title="Trading Management System", layout="wide")
st.title("Cryptocurrency Trading Management System")

# Database configuration
DB_CONFIG = {
    'server': '78.38.35.219',
    'database': 'G2',
    'username': 'G2',
    'password': 'g2',
    'driver': '{ODBC Driver 17 for SQL Server}',
    'timeout': 60
}


def create_connection():
    """Create and return a database connection with error handling"""
    try:
        conn_str = f"""
            DRIVER={DB_CONFIG['driver']};
            SERVER={DB_CONFIG['server']};
            DATABASE={DB_CONFIG['database']};
            UID={DB_CONFIG['username']};
            PWD={DB_CONFIG['password']};
            Connect Timeout={DB_CONFIG['timeout']};
        """
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        st.error(f"Database connection error: {str(e)}")
        return None


# Database fetch functions
def fetch_strategies():
    """Fetch list of trading strategies"""
    conn = create_connection()
    if not conn:
        return []

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT strategy_name FROM Strategy_Type")
            return [row[0] for row in cursor.fetchall()]
    except pyodbc.Error as e:
        st.error(f"Error fetching strategies: {str(e)}")
        return []
    finally:
        conn.close()


def fetch_currencies():
    """Fetch list of currencies"""
    conn = create_connection()
    if not conn:
        return []

    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT DISTINCT currency_symbol FROM Currencies")
            return [row[0] for row in cursor.fetchall()]
    except pyodbc.Error as e:
        st.error(f"Error fetching currencies: {str(e)}")
        return []
    finally:
        conn.close()


def fetch_strategy_currencies():
    """Fetch strategy-currency pairs"""
    conn = create_connection()
    if not conn:
        return {}

    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT sc.strategy_currency_id, 
                       st.strategy_name + ' - ' + c.currency_symbol AS display_name
                FROM Strategy_Currency sc
                JOIN Strategy_Type st ON sc.strategy_type_id = st.strategy_type_id
                JOIN Currencies c ON sc.currency_id = c.currency_id
            """)
            return {row[0]: row[1] for row in cursor.fetchall()}
    except pyodbc.Error as e:
        st.error(f"Error fetching strategy currencies: {str(e)}")
        return {}
    finally:
        conn.close()


# Load initial data
with st.spinner("Loading data..."):
    strategies = fetch_strategies()
    currencies = fetch_currencies()
    strategy_currencies = fetch_strategy_currencies()

# Main menu
menu = st.sidebar.selectbox("Menu", [
    "View Database Views",
    "Execute Functions",
    "Run Stored Procedures"
])

if menu == "View Database Views":
    st.header("Database Views")
    view_option = st.selectbox("Select View", [
        "Active Exchange Users",
        "Best Orders By Strategy-Currency",
        "User Exchange Trade Volume"
    ])

    conn = create_connection()
    if conn:
        try:
            with conn.cursor() as cursor:
                if view_option == "Active Exchange Users":
                    cursor.execute("SELECT * FROM ActiveExchangeUsers")
                    df = pd.DataFrame.from_records(cursor.fetchall(),
                                                   columns=[column[0] for column in cursor.description])
                    st.dataframe(df)

                elif view_option == "Best Orders By Strategy-Currency":
                    cursor.execute("SELECT * FROM Best_Order_By_Strategy_Currency")
                    df = pd.DataFrame.from_records(cursor.fetchall(),
                                                   columns=[column[0] for column in cursor.description])
                    st.dataframe(df)

                elif view_option == "User Exchange Trade Volume":
                    cursor.execute("SELECT * FROM User_Exchange_Trade_Volume")
                    df = pd.DataFrame.from_records(cursor.fetchall(),
                                                   columns=[column[0] for column in cursor.description])
                    st.dataframe(df)
        except pyodbc.Error as e:
            st.error(f"Error loading view: {str(e)}")
        finally:
            conn.close()

elif menu == "Execute Functions":
    st.header("Database Functions")
    function_option = st.selectbox("Select Function", [
        "Get Signal Count By Strategy/Currency",
        "Get Strategy Followers Count",
        "Get Users By Strategy And Currency"
    ])

    if function_option == "Get Signal Count By Strategy/Currency":
        st.subheader("Count Signals by Strategy and Currency")

        col1, col2 = st.columns(2)
        with col1:
            strategy_name = st.selectbox("Strategy", strategies)
        with col2:
            currency_symbol = st.selectbox("Currency", currencies)

        col3, col4 = st.columns(2)
        with col3:
            start_date = st.date_input("Start Date", datetime.now())
        with col4:
            end_date = st.date_input("End Date", datetime.now())

        if st.button("Execute"):
            conn = create_connection()
            if conn:
                try:
                    with conn.cursor() as cursor:
                        query = f"""
                        SELECT dbo.GetSignalCountByStrategyCurrencyAndDateRange(
                            '{strategy_name}', 
                            '{currency_symbol}', 
                            '{start_date}', 
                            '{end_date}'
                        ) AS SignalCount
                        """
                        cursor.execute(query)
                        result = cursor.fetchone()[0]
                        st.success(f"Signal Count: {result}")
                except pyodbc.Error as e:
                    st.error(f"Execution error: {str(e)}")
                finally:
                    conn.close()

    elif function_option == "Get Strategy Followers Count":
        st.subheader("Count Strategy Followers")
        strategy_name = st.selectbox("Strategy", strategies)

        if st.button("Execute"):
            conn = create_connection()
            if conn:
                try:
                    with conn.cursor() as cursor:
                        query = f"SELECT dbo.GetStrategyFollowersCount('{strategy_name}') AS FollowerCount"
                        cursor.execute(query)
                        result = cursor.fetchone()[0]
                        st.success(f"Followers: {result}")
                except pyodbc.Error as e:
                    st.error(f"Execution error: {str(e)}")
                finally:
                    conn.close()

    elif function_option == "Get Users By Strategy And Currency":
        st.subheader("Get Users by Strategy and Currency")

        col1, col2 = st.columns(2)
        with col1:
            strategy_name = st.selectbox("Strategy", strategies)
        with col2:
            currency_symbol = st.selectbox("Currency", currencies)

        if st.button("Execute"):
            conn = create_connection()
            if conn:
                try:
                    with conn.cursor() as cursor:
                        query = f"""
                        SELECT * FROM dbo.GetUsersByStrategyAndCurrency(
                            N'{strategy_name}', 
                            N'{currency_symbol}'
                        )
                        """
                        cursor.execute(query)
                        data = cursor.fetchall()
                        if data:
                            df = pd.DataFrame.from_records(data,
                                                           columns=[column[0] for column in cursor.description])
                            st.dataframe(df)
                            st.success(f"Users Found: {len(df)}")
                        else:
                            st.warning("No users found")
                except pyodbc.Error as e:
                    st.error(f"Execution error: {str(e)}")
                finally:
                    conn.close()

elif menu == "Run Stored Procedures":
    st.header("Stored Procedures")
    procedure_option = st.selectbox("Select Procedure", [
        "Add Exchange For User",
        "Add Strategy For User",
        "Enable User Strategy",
        "Disable User Strategy",
        "Delete Exchange"
    ])

    if procedure_option == "Add Exchange For User":
        st.subheader("Add Exchange to User")

        col1, col2 = st.columns(2)
        with col1:
            user_id = st.number_input("User ID", min_value=1, step=1)
        with col2:
            exchange_id = st.number_input("Exchange ID", min_value=1, step=1)

        api_key = st.text_input("API Key")
        api_secret = st.text_input("API Secret", type="password")
        total_balance = st.number_input("Total Balance", min_value=0.0)

        if st.button("Execute"):
            conn = create_connection()
            if conn:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(f"""
                        EXEC dbo.Add_Exchange_For_User
                            @UserId = {user_id},
                            @ExchangeID = {exchange_id},
                            @ApiKey = '{api_key}',
                            @ApiSecret = '{api_secret}',
                            @TotalBalance = {total_balance}
                        """)
                        conn.commit()
                        st.success("Exchange added successfully")
                except pyodbc.Error as e:
                    st.error(f"Error: {str(e)}")
                finally:
                    conn.close()

    elif procedure_option == "Add Strategy For User":
        st.subheader("Add Strategy to User")

        col1, col2 = st.columns(2)
        with col1:
            user_id = st.number_input("User ID", min_value=1, step=1)
        with col2:
            strategy_currency_id = st.selectbox(
                "Strategy-Currency",
                options=list(strategy_currencies.keys()),
                format_func=lambda x: strategy_currencies[x]
            )

        sub_balance = st.number_input("Sub Balance", min_value=0.0)

        if st.button("Add Strategy"):
            conn = create_connection()
            if conn:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(f"""
                        EXEC dbo.Add_Strategy_For_User
                            @UserID = {user_id},
                            @Strategy_Currency_ID = {strategy_currency_id},
                            @SubBalance = {sub_balance}
                        """)
                        conn.commit()
                        st.success("Strategy added successfully")
                except pyodbc.Error as e:
                    st.error(f"Error: {str(e)}")
                finally:
                    conn.close()

    elif procedure_option == "Enable User Strategy":
        st.subheader("Enable User Strategy")

        col1, col2 = st.columns(2)
        with col1:
            user_id = st.number_input("User ID", min_value=1, step=1)
        with col2:
            strategy_currency_id = st.selectbox(
                "Strategy-Currency",
                options=list(strategy_currencies.keys()),
                format_func=lambda x: strategy_currencies[x]
            )

        if st.button("Enable Strategy"):
            conn = create_connection()
            if conn:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(f"""
                        EXEC dbo.Enabling_User_Strategy
                            @UserID = {user_id},
                            @Strategy_Currency_ID = {strategy_currency_id}
                        """)
                        conn.commit()
                        st.success("Strategy enabled successfully")
                except pyodbc.Error as e:
                    st.error(f"Error: {str(e)}")
                finally:
                    conn.close()

    elif procedure_option == "Disable User Strategy":
        st.subheader("Disable User Strategy")

        col1, col2 = st.columns(2)
        with col1:
            user_id = st.number_input("User ID", min_value=1, step=1)
        with col2:
            strategy_currency_id = st.selectbox(
                "Strategy-Currency",
                options=list(strategy_currencies.keys()),
                format_func=lambda x: strategy_currencies[x]
            )

        if st.button("Disable Strategy"):
            conn = create_connection()
            if conn:
                try:
                    with conn.cursor() as cursor:
                        cursor.execute(f"""
                        EXEC dbo.Disabling_User_Strategy
                            @UserID = {user_id},
                            @Strategy_Currency_ID = {strategy_currency_id}
                        """)
                        conn.commit()
                        st.success("Strategy disabled successfully")
                except pyodbc.Error as e:
                    st.error(f"Error: {str(e)}")
                finally:
                    conn.close()

    elif procedure_option == "Delete Exchange":
        st.subheader("Delete Exchange")

        conn = create_connection()
        if conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT exchange_id, exchange_name FROM Exchanges")
                    exchanges = {row[0]: row[1] for row in cursor.fetchall()}

                    if exchanges:
                        exchange_id = st.selectbox(
                            "Select Exchange to Delete",
                            options=list(exchanges.keys()),
                            format_func=lambda x: f"{x} - {exchanges[x]}"
                        )

                        if st.button("Delete", type="primary"):
                            try:
                                cursor.execute(f"""
                                EXEC dbo.Delete_Exchange
                                    @ExchangeID = {exchange_id}
                                """)
                                conn.commit()
                                st.success("Exchange deleted successfully")
                            except pyodbc.Error as e:
                                st.error(f"Delete error: {str(e)}")
                    else:
                        st.warning("No exchanges available")
            except pyodbc.Error as e:
                st.error(f"Error loading exchanges: {str(e)}")
            finally:
                conn.close()

# Footer
st.sidebar.markdown("---")
st.sidebar.info("Cryptocurrency Trading System v1.1")