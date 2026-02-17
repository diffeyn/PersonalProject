USE MLS;

DROp TABLE players_general;

CREATE TABLE players_general (
    player_id INT NOT NULL PRIMARY KEY,
    name VARCHAR(30),
    height_cm INT,
    foot VARCHAR(5)
);

DROP TABLE teams;

CREATE TABLE teams (
    team_id INT NOT NULL PRIMARY KEY,
    team_name VARCHAR(30),
    team_abbr VARCHAR(5)
);

DROP TABLE matches;


CREATE TABLE matches (
    match_id VARCHAR(15) NOT NULL PRIMARY KEY,
    date DATE NOT NULL,
    home_team VARCHAR(5),
    away_team VARCHAR(5),
    home_score INT,
    away_score INT
);

drop table matches_stats;

CREATE TABLE matches_stats (
    match_id VARCHAR(15) NOT NULL PRIMARY KEY,
    general_possession_pct_home FLOAT(6,3),
    general_possession_pct_away FLOAT(6,3),
    general_shots_home INT,
    general_shots_away INT,
    general_shots_on_goal_home INT,
    general_shots_on_goal_away INT,
    general_blocked_shots_home INT,
    general_blocked_shots_away INT,
    general_blocked_home INT,
    general_blocked_away INT,
    general_total_passes_home INT,
    general_total_passes_away INT,
    general_passing_accuracy_pct_home FLOAT(6,3),
    general_passing_accuracy_pct_away FLOAT(6,3),
    general_corners_home INT,
    general_corners_away INT,
    general_total_crosses_home INT,
    general_total_crosses_away INT,
    general_offsides_home INT,
    general_offsides_away INT,
    general_aerial_duels_won_home INT,
    general_aerial_duels_won_away INT,
    general_expected_goals_home FLOAT(6,3),
    general_expected_goals_away FLOAT(6,3),
    general_goalkeeper_saves_home INT,
    general_goalkeeper_saves_away INT,
    general_clearances_home INT,
    general_clearances_away INT,
    general_fouls_home INT,
    general_fouls_away INT,
    general_yellow_cards_home INT,
    general_yellow_cards_away INT,
    general_red_cards_home INT,
    general_red_cards_away INT,
    general_overall_pct_home FLOAT(6,3),
    general_overall_pct_away FLOAT(6,3),
    general_open_play_pass_pct_home FLOAT(6,3),
    general_open_play_pass_pct_away FLOAT(6,3),
    general_set_piece_cross_pct_home FLOAT(6,3),
    general_set_piece_cross_pct_away FLOAT(6,3),
    general_open_play_cross_pct_home FLOAT(6,3),
    general_open_play_cross_pct_away FLOAT(6,3),
    shooting_goals_home INT,
    shooting_goals_away INT,
    shooting_on_target_home INT,
    shooting_on_target_away INT,
    shooting_off_target_home INT,
    shooting_off_target_away INT,
    shooting_blocked_home INT,
    shooting_blocked_away INT,
    general_goals_conceded_home INT,
    general_goals_conceded_away INT,
    general_shots_against_home INT,
    general_shots_against_away INT,
    general_clean_sheets_home INT,
    general_clean_sheets_away INT,
    general_xg_conceded_home FLOAT(6,3),
    general_xg_conceded_away FLOAT(6,3),
    general_interceptions_home INT,
    general_interceptions_away INT,
    passing_overall_pct_home FLOAT(6,3),
    passing_overall_pct_away FLOAT(6,3),
    passing_open_play_pass_pct_home FLOAT(6,3),
    passing_open_play_pass_pct_away FLOAT(6,3),
    passing_set_piece_cross_pct_home FLOAT(6,3),
    passing_set_piece_cross_pct_away FLOAT(6,3),
    passing_open_play_cross_pct_home FLOAT(6,3),
    passing_open_play_cross_pct_away FLOAT(6,3),
    possession_0_5_home FLOAT(6,3),
    possession_0_5_away FLOAT(6,3),
    possession_6_10_home FLOAT(6,3),
    possession_6_10_away FLOAT(6,3),
    possession_11_15_home FLOAT(6,3),
    possession_11_15_away FLOAT(6,3),
    possession_16_20_home FLOAT(6,3),
    possession_16_20_away FLOAT(6,3),
    possession_21_25_home FLOAT(6,3),
    possession_21_25_away FLOAT(6,3),
    possession_26_30_home FLOAT(6,3),
    possession_26_30_away FLOAT(6,3),
    possession_31_35_home FLOAT(6,3),
    possession_31_35_away FLOAT(6,3),
    possession_36_40_home FLOAT(6,3),
    possession_36_40_away FLOAT(6,3),
    possession_41_45_home FLOAT(6,3),
    possession_41_45_away FLOAT(6,3),
    possession_46_50_home FLOAT(6,3),
    possession_46_50_away FLOAT(6,3),
    possession_51_55_home FLOAT(6,3),
    possession_51_55_away FLOAT(6,3),
    possession_56_60_home FLOAT(6,3),
    possession_56_60_away FLOAT(6,3),
    possession_61_65_home FLOAT(6,3),
    possession_61_65_away FLOAT(6,3),
    possession_66_70_home FLOAT(6,3),
    possession_66_70_away FLOAT(6,3),
    possession_71_75_home FLOAT(6,3),
    possession_71_75_away FLOAT(6,3),
    possession_76_80_home FLOAT(6,3),
    possession_76_80_away FLOAT(6,3),
    possession_81_85_home FLOAT(6,3),
    possession_81_85_away FLOAT(6,3),
    possession_86_90_home FLOAT(6,3),
    possession_86_90_away FLOAT(6,3),
    xg_total_team_xg_home FLOAT(6,3),
    xg_total_team_xg_away FLOAT(6,3),
    xg_shots_home INT,
    xg_shots_away INT,
    xg_shots_on_target_home INT,
    xg_shots_on_target_away INT
);

drop table match_events;

CREATE TABLE match_events (
    event_id INT NOT NULL,
    match_id VARCHAR(15) NOT NULL,
    event_minute VARCHAR(10),
    event_type VARCHAR(20),
    event_comment VARCHAR(300)
);

DROP TABLE match_player_stats;

CREATE TABLE match_player_stats (
    match_id VARCHAR(15) NOT NULL,
    player_id INT NOT NULL,
    player_name VARCHAR(30),
    club VARCHAR(5),
    side VARCHAR(5),
    minutes INT,
    goals INT,
    expected_goals FLOAT(6, 3),
    shot_conv_perc INT,
    on_target INT,
    pass_perc FLOAT(6, 3),
    assists INT,
    passes INT,
    `cross` INT,
    corner_kick INT,
    key_pass INT,
    aerial INT,
    aerial_perc FLOAT(6,3),
    fouls INT,
    fouls_against INT,
    offside INT,
    yellow_card INT,
    red_card INT,
    gk_goals_saved INT,
    gk_goals_against INT,
    gk_expected_goals_against FLOAT(6,3),
    gk_pass FLOAT(6,3),
    gk_throws INT,
    gk_long_balls INT,
    gk_launches INT,
    GK INT,
    corners_conceded INT,
    PRIMARY KEY (match_id, player_id) 
)


DROP TABLE player_finance;

CREATE TABLE player_finance (
    date DATE NOT NULL,
    player_id int NOT NULL,
    wage_eur INT,
    value_eur INT,
    PRIMARY KEY (date, player_id)
)

DROP TABLE player_stats;

CREATE TABLE player_stats (
    date DATE NOT NULL,
    player_id int NOT NULL,
    age INT,
    jersey_num INT,
    overall_rating INT,
    best_overall INT,
    best_position VARCHAR(15),
    total_attacking INT,
    crossing INT,
    finishing INT,
    heading_accuracy INT,
    short_passing INT,
    volleys INT,
    total_skill INT,
    dribbling INT,
    curve INT,
    fk_accuracy INT,
    long_passing INT,
    ball_control INT,
    total_movement INT,
    acceleration INT,
    sprint_speed INT,
    agility INT,
    reactions INT,
    balance INT,
    total_power INT,
    shot_power INT,
    jumping INT,
    stamina INT,
    long_shots INT,
    total_mentality INT,
    aggression INT,
    interceptions INT,
    attack_position INT,
    vision INT,
    penalties INT,
    composure INT,
    total_defending INT,
    defensive_awareness INT,
    standing_tackle INT,
    sliding_tackle INT,
    total_goalkeeping INT,
    gk_diving INT,
    gk_handling INT,
    gk_kicking INT,
    gk_positioning INT,
    gk_reflexes INT,
    marking INT,
    tactical_awareness INT,
    positioning INT,
    tackling INT,
    PRIMARY KEY(date, player_id)
)

DROP TABLE team_roster;

CREATE TABLE team_roster (
    player_id INT NOT NULL,
    stint_id INT NOT NULL,
    team_id INT NOT NULL,
    stint_start DATE,
    stint_end DATE,
    days_observed INT,
    obs_count INT,
    PRIMARY KEY (player_id, stint_id)
)

DROP TABLE team_stats;

CREATE TABLE team_stats (
    date date NOT NULL,
    team_id INT NOT NULL,
    name VARCHAR(50),
    formation_base VARCHAR(15),
    formation_style VARCHAR(15),
    overall INT,
    attack INT,
    midfield INT,
    defence INT,
    club_worth VARCHAR(15),
    players INT,
    PRIMARY KEY(date, team_id)
)

