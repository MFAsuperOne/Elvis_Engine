from json import dump, loads
import re

import numpy as np

from constants import (
    EASY, MEDIUM, HARD, SPECIFIC_TO_MAIN_MAPPING, VOWELS,
)


def print_msg(msg, stars_count=1):
    print(f'[{"*"*stars_count}] {msg}')


def dump_dict_as_json(path_to_json, dict):
    with open(path_to_json, 'w', encoding='utf8') as f:
        dump(dict, f, indent=2, ensure_ascii=False)


def load_json_as_dict(path_to_json):
    with open(path_to_json, 'r', encoding='utf8') as json_as_file:
        json_as_str = json_as_file.read()
    json_as_dict = loads(json_as_str)
    return json_as_dict


def select_query(cursor, select_, from_, where_='', db_name='superleague'):
    query = f'SELECT {select_} FROM {db_name}.{from_}'
    if where_ != '':
        query += f' WHERE {where_}'
    cursor.execute(query)
    out = cursor.fetchall()
    return out


def insert_into_query(cursor, into_, columns, values, db_name='superleague'):
    query = f'INSERT INTO {db_name}.{into_} ({", ".join(columns)}) '
    query += f'VALUES {values}'
    cursor.execute(query)


def update_query(
        cursor, table, columns, values, where_, db_name='superleague'
):
    query = f'UPDATE {db_name}.{table} '
    query += 'SET ' + ', '.join(
        f'{c} = {v.__repr__()}' for c, v in zip(columns, values)
    )
    query += f' WHERE {where_}'
    cursor.execute(query)


def resolve_counts(count):
    positive_count = count // 2 + count % 2
    negative_count = count // 2
    return positive_count, negative_count


def make_questions_from_positive_and_negative(positive, negative):
    questions = merge_dicts(
        assign_answers_to_questions(positive, True),
        assign_answers_to_questions(negative, False)
    )
    return questions


def merge_dicts(left, right):
    out = {k: v for k, v in left.items()}
    out.update(right)
    return out


def assign_answers_to_questions(questions, answer):
    for single_question in questions.keys():
        questions[single_question]['answer'] = answer
    return questions


def generate_random_player_team_couples(input_team_id, count, assignment):
    team_assignment = [
        (player_id, team_id) for player_id, team_id in assignment
        if team_id == input_team_id
    ]
    if len(team_assignment) > count:
        couples_indices = np.random.choice(
            list(range(len(team_assignment))), count, replace=False
        )
    else:
        couples_indices = range(len(team_assignment))
    couples = [team_assignment[i] for i in couples_indices]
    return couples


def append_question(
        questions, single_question, tags, parent_tags, difficulty, template
):
    std_template = template.replace('$LEAGUE', '$COMPETITION')
    std_template = std_template.replace('$FROMTEAM', '$TEAM')
    std_template = std_template.replace('$TOTEAM', '$TEAM')
    std_template = std_template.replace('$WINNER', '$TEAM')
    std_template = std_template.replace('$NOWINNER', '$TEAM')
    std_template = std_template.replace('$AGAINSTTEAM', '$TEAM')
    std_template = std_template.replace('$MNUMBER', '$NUMBER')
    std_template = std_template.replace('$SNUMBER', '$NUMBER')
    questions[single_question] = {
        'tags': tags,
        'parent_tags': parent_tags,
        'difficulty': difficulty,
        'template': std_template,
    }


def all_lists_are_not_empty(lists):
    return all(len(single_list) > 0 for single_list in lists)


def get_var_count(template):
    if isinstance(template, str):
        return template.count('$')
    else:
        return max(get_var_count(sub_t) for sub_t in template.values())


def resolve_position(actual_position):
    actual_lower = actual_position.lower()
    out_position = SPECIFIC_TO_MAIN_MAPPING.get(actual_lower, actual_lower)
    return out_position


def choose_random_templates(templates, count):
    if len(templates) > 0:
        return list(np.random.choice(templates, count))
    else:
        return []


def cut_off_and_season_name_do_not_match(
        cut_off_left, cut_off_right, season_name
):
    return not is_cut_off_and_season_name_match(
        cut_off_left, cut_off_right, season_name
    )


def is_cut_off_and_season_name_match(cut_off_left, cut_off_right, season_name):
    season_first_year = int(season_name.split('-')[0])
    return cut_off_left <= season_first_year <= cut_off_right


def is_vowel_start(s):
    return any(s.lower().startswith(v) for v in VOWELS)


def is_league_with_article(league):
    articles = ('la', 'le',)
    return any(a in league.lower().split(' ') for a in articles)


def assess_difficulty(template, season='', position=''):
    if get_var_count(template) > 3:
        if season != '':
            year = int(season.split('-')[0])
            if year < 2010:
                return HARD
        else:
            return HARD
    elif get_var_count(template) <= 2:
        if season != '':
            year = int(season.split('-')[0])
            if year >= 2015:
                return EASY
        else:
            return EASY
    return MEDIUM


def generate_random_difficulty():
    return np.random.choice([EASY, MEDIUM, HARD])


def shorten_season(season):
    if '-' in season:
        first_year = season.split('-')[0]
        second_year = season.split('-')[1]
        return f'{first_year}/{second_year[2:]}'
    else:
        return season


def is_variable_in_template(template, variable):
    if isinstance(template, str):
        return variable in template
    else:
        return any(
            is_variable_in_template(t, variable) for t in template.values()
        )


def get_first_year_from_season_name(season_name):
    if '-' in season_name:
        year = season_name.split('-')[0]
    else:
        year = season_name
    year = int(year)
    return year


def format_player_tag(name, birth_date):
    if birth_date is None:
        return name
    return f'{name}_{format_birth_date(birth_date)}'


def format_birth_date(birth_date):
    return birth_date.strftime('%d.%m.%Y')


def preprocess_player_name(player_name):
    no_ap = re.findall(r'[A-Z]{2}', player_name)
    for single_no_ap in no_ap:
        if len(single_no_ap) != 2:
            continue
        first_ch, second_ch = single_no_ap
        player_name = player_name.replace(
            single_no_ap, f'{first_ch}\'{second_ch}'
        )
    return player_name


def strip_if_not_none(str_):
    if str_ is None:
        return None
    return str_.strip()
