import sys

from db.superleague import (
    setup_connection_to_superleague,
    close_connection_to_superleague
)
from utils import merge_dicts, print_msg, dump_dict_as_json
from generate_industrial import (
    generate_player_played_in_team,
    generate_player_played_in_team_as_pos,
    generate_player_played_in_team_as_pos_at_league_season,
    generate_player_has_never_played_in_team_as_of_limiter,
    generate_player_played_in_team_as_of_limiter,
    generate_former_team_player_as_of_limiter,
    generate_player_wore_shirt_for_team_at_season,
    generate_player_wore_shirt_for_team_as_of_limiter,
    generate_team_vs_team_at_season_in_stadium,
    generate_season_with_final_result,
    generate_season_without_final_result,
    generate_winner_beat_no_winner_in_season,
    generate_team_played_in_league_as_of_limiter,
    generate_team_win_league_as_of_limiter,
    generate_team_never_win_league_as_of_limiter,
    generate_player_moved_to_team_from_team,
    generate_player_joined_or_left_team_in_year,
    generate_player_scored_for_team,
    generate_player_scored_more_than_number_goals,
    generate_player_scored_more_than_number_goals_in_season,
    generate_player_scored_for_team_as_of_limiter,
    generate_player_scored_for_team_as_of_limiter_in_league,
    generate_player_played_more_than_number,
    generate_player_played_less_than_number,
    generate_player_win_league_with_team,
    generate_player_win_league_with_team_limiter,
    generate_player_was_team_top_scorer,
)
from generate_industrial.tables import (
    get_teams_abbreviations_dict, get_leagues_abbreviations_dict
)

TEAM_ID = 53
TOTAL_COUNT = 4000
CUT_OFF_LEFT = {
    52: 2014,
    54: 2014,
    'default': 1990
}
CUT_OFF_RIGHT = {
    'default': 2022
}


def run_industrial_generation(team_id, total_count):
    counts = __create_questions_counts(total_count)
    question_generators = (
        generate_player_played_in_team,
        generate_player_played_in_team_as_pos,
        generate_player_played_in_team_as_pos_at_league_season,
        generate_player_has_never_played_in_team_as_of_limiter,
        generate_player_played_in_team_as_of_limiter,
        generate_former_team_player_as_of_limiter,
        generate_player_wore_shirt_for_team_at_season,
        generate_player_wore_shirt_for_team_as_of_limiter,
        generate_team_vs_team_at_season_in_stadium,
        generate_season_with_final_result,
        generate_season_without_final_result,
        generate_winner_beat_no_winner_in_season,
        generate_team_played_in_league_as_of_limiter,
        generate_team_win_league_as_of_limiter,
        generate_team_never_win_league_as_of_limiter,
        generate_player_moved_to_team_from_team,
        generate_player_joined_or_left_team_in_year,
        generate_player_scored_for_team,
        generate_player_scored_more_than_number_goals,
        generate_player_scored_more_than_number_goals_in_season,
        generate_player_scored_for_team_as_of_limiter,
        generate_player_scored_for_team_as_of_limiter_in_league,
        generate_player_played_more_than_number,
        generate_player_played_less_than_number,
        generate_player_win_league_with_team,
        generate_player_win_league_with_team_limiter,
        generate_player_was_team_top_scorer,
    )
    questions = {}
    for single_question_generator in question_generators:
        print_msg(f'Running {single_question_generator.__name__}')
        new_questions = single_question_generator(
            team_id, CUT_OFF_LEFT, CUT_OFF_RIGHT,
            counts[single_question_generator.__name__]
        )
        new_count = len(new_questions)
        print_msg(f'Questions generated with this generator: {new_count}')
        questions = merge_dicts(questions, new_questions)
    questions = __post_process_tags(questions)
    return questions


def __create_questions_counts(total_count):
    counts = {
        'generate_player_played_in_team': 0.008 * total_count,
        'generate_player_played_in_team_as_pos': 0.03 * total_count,
        'generate_player_played_in_team_as_pos_at_league_season': 0.151 * total_count,
        'generate_player_has_never_played_in_team_as_of_limiter': 0.01 * total_count,
        'generate_player_played_in_team_as_of_limiter': 0.044 * total_count,
        'generate_former_team_player_as_of_limiter': 0.008 * total_count,
        'generate_player_wore_shirt_for_team_at_season': 0.065 * total_count,
        'generate_player_wore_shirt_for_team_as_of_limiter': 0.01 * total_count,
        'generate_team_vs_team_at_season_in_stadium': 0.096 * total_count,
        'generate_season_with_final_result': 0.07 * total_count,
        'generate_season_without_final_result': 0.07 * total_count,
        'generate_winner_beat_no_winner_in_season': 0.05 * total_count,
        'generate_team_played_in_league_as_of_limiter': 0.01 * total_count,
        'generate_team_win_league_as_of_limiter': 0.01 * total_count,
        'generate_team_never_win_league_as_of_limiter': 0.01 * total_count,
        'generate_player_moved_to_team_from_team': 0.05 * total_count,
        'generate_player_joined_or_left_team_in_year': 0.04 * total_count,
        'generate_player_scored_for_team': 0.03 * total_count,
        'generate_player_scored_more_than_number_goals': 0.025 * total_count,
        'generate_player_scored_more_than_number_goals_in_season': 0.025 * total_count,
        'generate_player_scored_for_team_as_of_limiter': 0.005 * total_count,
        'generate_player_scored_for_team_as_of_limiter_in_league': 0.01 * total_count,
        'generate_player_played_more_than_number': 0.0175 * total_count,
        'generate_player_played_less_than_number': 0.0175 * total_count,
        'generate_player_win_league_with_team': 0.05 * total_count,
        'generate_player_win_league_with_team_limiter': 0.01 * total_count,
        'generate_player_was_team_top_scorer': 0.078 * total_count,
    }
    counts = {k: int(v) for k, v in counts.items()}
    return counts


def __post_process_tags(questions):
    connection, cursor = setup_connection_to_superleague()
    teams_abbreviations = get_teams_abbreviations_dict(cursor)
    leagues_abbreviations = get_leagues_abbreviations_dict(cursor)
    for single_q in questions.keys():
        tags = questions[single_q]['tags']
        processed_tags = []
        for i in range(len(tags)):
            single_tag = tags[i]
            single_tag = teams_abbreviations.get(single_tag, single_tag)
            single_tag = leagues_abbreviations.get(single_tag, single_tag)
            if single_tag is not None and single_tag.strip() != '':
                processed_tags.append(single_tag)
        questions[single_q]['tags'] = processed_tags
    close_connection_to_superleague(connection, cursor)
    return questions


if __name__ == '__main__':
    questions = run_industrial_generation(TEAM_ID, TOTAL_COUNT)
    print_msg(
        f'Questions are ready. Totally generated: {len(questions)} questions.'
    )
    dump_dict_as_json('industrial_atletico_v0.5.json', questions)
