#!/usr/bin/env python
import streamlit as st
import sqlite3
import re
import requests

# Ruta a la base de datos SQLite (se usará todo el dataset ubicado aquí, con todas sus tablas y registros)
DB_PATH = # Ingresa Ruta a la base de datos SQLite
# API Key de Deep Seek
API_KEY = # Ingresa API Key de Deep Seek

# Mapeo simple para identificar equipos por nombre común (se pueden agregar más)
TEAM_MAPPING = {
    "bulls": "CHI",
    "lakers": "LAL",
    "celtics": "BOS",
    "hawks": "ATL",
    "spurs": "SAS",
    # Agrega otros equipos según sea necesario
}

def init_connection():
    return sqlite3.connect(DB_PATH)

def deep_seek_query(user_query, connection):
    """
    Llama a la API de Deep Seek para transformar una consulta en lenguaje natural
    a una consulta SQL. La consulta resultante se ejecuta contra el dataset local
    que incluye TODAS las tablas y registros.
    """
    endpoint = # Endpoint supuesto para Deep Seek
    headers = {
         "Content-Type": "application/json",
         "Authorization": f"Bearer {API_KEY}"
    }
    # Se incluye el esquema completo de la base de datos (todas las tablas y sus columnas)
    payload = {
        "query": user_query,
        "database_schema": (
            "game(season_id, team_id_home, team_abbreviation_home, team_name_home, game_id, game_date, matchup_home, wl_home, min, fgm_home, "
            "fga_home, fg_pct_home, fg3m_home, fg3a_home, fg3_pct_home, ftm_home, fta_home, ft_pct_home, oreb_home, dreb_home, reb_home, "
            "ast_home, stl_home, blk_home, tov_home, pf_home, pts_home, plus_minus_home, video_available_home, team_id_away, team_abbreviation_away, "
            "team_name_away, matchup_away, wl_away, fgm_away, fga_away, fg_pct_away, fg3m_away, fga_away, fg3_pct_away, ftm_away, fta_away, "
            "ft_pct_away, oreb_away, dreb_away, reb_away, ast_away, stl_away, blk_away, tov_away, pf_away, pts_away, plus_minus_away, season_type); "
            "game_summary(game_date_est, game_sequence, game_id, game_status_id, game_status_text, gamecode, home_team_id, visitor_team_id, season, "
            "live_period, live_pc_time, natl_tv_broadcaster_abbreviation, live_period_time_bcast, wh_status); "
            "other_stats(game_id, league_id, team_id_home, team_abbreviation_home, team_city_home, pts_paint_home, pts_2nd_chance_home, pts_fb_home, "
            "largest_lead_home, lead_changes, times_tied, team_turnovers_home, total_turnovers_home, team_rebounds_home, pts_off_to_home, team_id_away, "
            "team_abbreviation_away, team_city_away, pts_paint_away, pts_2nd_chance_away, pts_fb_away, largest_lead_away, team_turnovers_away, "
            "total_turnovers_away, team_rebounds_away, pts_off_to_away); "
            "officials(game_id, official_id, first_name, last_name, jersey_num); "
            "inactive_players(game_id, player_id, first_name, last_name, jersey_num, team_id, team_city, team_name, team_abbreviation); "
            "game_info(game_id, game_date, attendance, game_time); "
            "line_score(game_date_est, game_sequence, game_id, team_id_home, team_abbreviation_home, team_city_name_home, team_nickname_home, "
            "team_wins_losses_home, pts_qtr1_home, pts_qtr2_home, pts_qtr3_home, pts_qtr4_home, pts_ot1_home, pts_ot2_home, pts_ot3_home, pts_ot4_home, "
            "pts_ot5_home, pts_ot6_home, pts_ot7_home, pts_ot8_home, pts_home, team_id_away, team_abbreviation_away, team_city_name_away, team_nickname_away, "
            "team_wins_losses_away, pts_qtr1_away, pts_qtr2_away, pts_qtr3_away, pts_qtr4_away, pts_ot1_away, pts_ot2_away, pts_ot3_away, pts_ot4_away, "
            "pts_ot5_away, pts_ot6_away, pts_ot7_away, pts_ot8_away, pts_away); "
            "player(id, full_name, first_name, last_name, is_active); "
            "team(id, full_name, abbreviation, nickname, city, state, year_founded); "
            "common_player_info(person_id, first_name, last_name, display_first_last, display_last_comma_first, display_fi_last, player_slug, birthdate, "
            "school, country, last_affiliation, height, weight, season_exp, jersey, position, rosterstatus, games_played_current_season_flag, team_id, "
            "team_name, team_abbreviation, team_code, team_city, playercode, from_year, to_year, dleague_flag, nba_flag, games_played_flag, draft_year, "
            "draft_round, draft_number, greatest_75_flag); "
            "team_details(team_id, abbreviation, nickname, yearfounded, city, arena, arenacapacity, owner, generalmanager, headcoach, dleagueaffiliation, "
            "facebook, instagram, twitter); "
            "team_history(team_id, city, nickname, year_founded, year_active_till); "
            "draft_combine_stats(season, player_id, first_name, last_name, player_name, position, height_wo_shoes, height_wo_shoes_ft_in, height_w_shoes, "
            "height_w_shoes_ft_in, weight, wingspan, wingspan_ft_in, standing_reach, standing_reach_ft_in, body_fat_pct, hand_length, hand_width, "
            "standing_vertical_leap, max_vertical_leap, lane_agility_time, modified_lane_agility_time, three_quarter_sprint, bench_press, "
            "spot_fifteen_corner_left, spot_fifteen_break_left, spot_fifteen_top_key, spot_fifteen_break_right, spot_fifteen_corner_right, "
            "spot_college_corner_left, spot_college_break_left, spot_college_top_key, spot_college_break_right, spot_college_corner_right, "
            "spot_nba_corner_left, spot_nba_break_left, spot_nba_top_key, spot_nba_break_right, spot_nba_corner_right, off_drib_fifteen_break_left, "
            "off_drib_fifteen_top_key, off_drib_fifteen_break_right, off_drib_college_break_left, off_drib_college_top_key, off_drib_college_break_right, "
            "on_move_fifteen, on_move_college); "
            "draft_history(person_id, player_name, season, round_number, round_pick, overall_pick, draft_type, team_id, team_city, team_name, "
            "team_abbreviation, organization, organization_type, player_profile_flag); "
            "team_info_common; "
            "nba_salaries(full_name, Salary, Position, Age, Team, PTS, player_id, is_active)"
        )
    }
    try:
        response = requests.post(endpoint, json=payload, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            sql_query = data.get("sql_query")
            if sql_query:
                try:
                    cur = connection.cursor()
                    cur.execute(sql_query)
                    rows = cur.fetchall()
                    results_text = ""
                    for row in rows:
                        results_text += str(row) + "\n"
                    return f"Consulta SQL generada: {sql_query}\nResultados:\n{results_text}"
                except Exception as e:
                    return "Error al ejecutar la consulta derivada de Deep Seek: " + str(e)
            else:
                return "No se obtuvo una consulta SQL desde Deep Seek."
        else:
            return "Error en la llamada a Deep Seek: " + response.text
    except Exception as e:
        return "Excepción al llamar a Deep Seek: " + str(e)

def process_query(query, connection):
    """
    Procesa la consulta del usuario aplicando reglas manuales para ciertos casos (por ejemplo, victorias, promedios o históricos).
    Si la lógica manual no cubre la consulta, se delega a Deep Seek para obtener la consulta SQL correspondiente.
    """
    query_lower = query.lower()
    try:
        cursor = connection.cursor()
        if "victorias" in query_lower:
            # Determinar si se especifica el campo local (casa) o visitante
            location_condition = ""
            if "casa" in query_lower or "local" in query_lower:
                location_condition = "home"
            elif "visitante" in query_lower or "fuera" in query_lower:
                location_condition = "away"
            # Identificar el equipo a través del mapeo
            team_key_found = None
            for team_key in TEAM_MAPPING.keys():
                if team_key in query_lower:
                    team_key_found = team_key
                    break
            if team_key_found:
                team_code = TEAM_MAPPING[team_key_found]
                year_match = re.search(r'\b(20\d{2})\b', query)
                year_filter = ""
                if year_match:
                    year = year_match.group(1)
                    year_filter = f" AND game_date LIKE '{year}%'"
                if location_condition == "home":
                    sql = ("SELECT COUNT(*) FROM game WHERE team_abbreviation_home = ? AND wl_home = 'W'" + year_filter)
                    cursor.execute(sql, (team_code,))
                elif location_condition == "away":
                    sql = ("SELECT COUNT(*) FROM game WHERE team_abbreviation_away = ? AND wl_away = 'W'" + year_filter)
                    cursor.execute(sql, (team_code,))
                else:
                    sql = ("SELECT (SELECT COUNT(*) FROM game WHERE team_abbreviation_home = ? AND wl_home = 'W'" + year_filter +
                           ") + (SELECT COUNT(*) FROM game WHERE team_abbreviation_away = ? AND wl_away = 'W'" + year_filter +
                           ") AS total_wins")
                    cursor.execute(sql, (team_code, team_code))
                count = cursor.fetchone()[0]
                loc_text = " en casa" if location_condition == "home" else (" de visitante" if location_condition == "away" else "")
                year_text = f" en el año {year}" if year_match else ""
                return f"El equipo {team_key_found.capitalize()} (código {team_code}) obtuvo {count} victorias{loc_text}{year_text}."
        elif "promedio" in query_lower and "puntos" in query_lower:
            sql = "SELECT AVG(pts_home) AS avg_home, AVG(pts_away) AS avg_away FROM game"
            cursor.execute(sql)
            row = cursor.fetchone()
            if row and row[0] is not None and row[1] is not None:
                return ("Promedio de puntos:\n"
                        f" - Casa: {row[0]:.2f}\n"
                        f" - Visitante: {row[1]:.2f}")
            else:
                return "No se encontraron datos para calcular el promedio de puntos."
        elif "histórico" in query_lower or "historial" in query_lower:
            sql = "SELECT COUNT(*) FROM game_summary"
            cursor.execute(sql)
            count = cursor.fetchone()[0]
            return f"El historial registra un total de {count} juegos."
        elif "predicción" in query_lower or "predecir" in query_lower:
            return "La funcionalidad de predicción y análisis avanzado está en desarrollo."
    except Exception as e:
        return "Error durante el procesamiento de la consulta: " + str(e)
    
    # Si ninguna regla manual aplica, se delega la consulta a Deep Seek
    return deep_seek_query(query, connection)

def main():
    st.title("Chatbox Asistido por IA - NBA")
    if "chat_log" not in st.session_state:
        st.session_state.chat_log = (
            "Bienvenido al chatbox de análisis de datos NBA.\n"
            "Ejemplos de consulta:\n"
            "- ¿Cuál es el promedio de puntos de casa y visitante?\n"
            "- ¿Cuántas victorias tienen los Bulls en casa en el año 2020?\n"
            "- ¿Cuál es el histórico de juegos registrados?\n"
            "- ¿Predicciones de algún partido? (Funcionalidad en desarrollo)\n\n"
        )
    connection = init_connection()
    st.text_area("Conversación", value=st.session_state.chat_log, height=300)
    user_input = st.text_input("Escribe tu consulta:")
    if st.button("Enviar") and user_input.strip():
        st.session_state.chat_log += "Usuario: " + user_input + "\n"
        respuesta = process_query(user_input, connection)
        st.session_state.chat_log += "IA: " + respuesta + "\n\n"
        # Comentamos st.experimental_rerun() para evitar errores si no está disponible en esta versión de Streamlit
        # st.experimental_rerun()

if __name__ == '__main__':
    main()