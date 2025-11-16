-- fighters: one row per fighter
CREATE TABLE fighters (
    fighter_id TEXT PRIMARY KEY,   -- e.g. '9c897ac0ff08c9cd' from UFCStats URL
    name       TEXT NOT NULL       -- 'Islam Makhachev'
);

-- fights: one row per fight
CREATE TABLE fights (
    fight_id   TEXT PRIMARY KEY,             -- '8db1b36dde268ef6' from UFCStats fight URL

    event_name TEXT NOT NULL,               -- 'UFC 322: Della Maddalena vs Makhachev'
    event_date DATE NOT NULL,               
    weight_class TEXT,                      -- 'Welterweight', 'Lightweight', etc.

    fighter1_id TEXT NOT NULL REFERENCES fighters(fighter_id),
    fighter2_id TEXT NOT NULL REFERENCES fighters(fighter_id),

    winner_id   TEXT REFERENCES fighters(fighter_id),  -- NULL for draw/NC/upcoming

    method      TEXT,                        -- 'KO/TKO', 'SUB', 'DEC', 'NC', etc.
    round_ended INTEGER,                     -- round the fight ended in
    time_ended  TEXT,                        -- '3:22'

    -- wave 1 odds (decimal) 
    fighter1_closing_odds DOUBLE PRECISION,
    fighter2_closing_odds DOUBLE PRECISION,

    created_at TIMESTAMPTZ DEFAULT now()
);

-- fighter_stats: one row per fighter per fight
CREATE TABLE fighter_stats (
    fight_id  TEXT NOT NULL REFERENCES fights(fight_id) ON DELETE CASCADE,
    fighter_id TEXT NOT NULL REFERENCES fighters(fighter_id) ON DELETE CASCADE,

    is_winner BOOLEAN,                     -- set from fights.winner_id

    knockdowns            INTEGER,
    sig_strikes_landed    INTEGER,
    sig_strikes_attempted INTEGER,
    total_strikes_landed  INTEGER,
    total_strikes_attempted INTEGER,
    td_landed             INTEGER,
    td_attempts           INTEGER,
    sub_attempts          INTEGER,
    control_time_seconds  INTEGER,         

    time_fought_seconds   INTEGER,

    PRIMARY KEY (fight_id, fighter_id)
);
