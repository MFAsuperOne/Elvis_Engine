import os
from datetime import datetime
import ast
from json import loads
from unicodedata import normalize

from airtable import airtable, AirtableError
from tqdm import tqdm
import numpy as np

from db.templates import (
    setup_connection_to_templates_db,
    close_connection_to_templates_db
)
from run_industrial_generation import run_industrial_generation
from utils import select_query, insert_into_query, update_query, print_msg

AIRTABLE_API_KEY = os.environ.get('AIRTABLE_API_KEY')


def lambda_handler(event, context):
    input_params = __extract_input_parameters(event, context)
    (parent_tag_record_id, team_id, total_count, airtable_base_id,
     questions_table_id, tags_table_id) = input_params
    existing_questions = __get_not_used_questions_from_rds(
        parent_tag_record_id
    )
    total_count_to_generate = total_count - len(existing_questions)
    if total_count_to_generate >= 0:
        output_questions = existing_questions.copy()
        new_questions_to_use = __generate_new_questions_to_use_and_add_to_rds(
            parent_tag_record_id, team_id, total_count_to_generate,
            airtable_base_id, tags_table_id
        )
        output_questions += new_questions_to_use
        __update_questions_as_used(existing_questions)
    else:
        output_questions = existing_questions[:total_count]
        __update_questions_as_used(output_questions)
    print_msg('Sending generated questions to AirTable.')
    print_msg(f'Total count to send: {len(output_questions)}.')
    sent_questions_count = __send_questions_to_airtable(
        airtable_base_id, questions_table_id, output_questions
    )
    print_msg(f'Total count successfully sent: {sent_questions_count}')


def __extract_input_parameters(event, context):
    print_msg('Received input')
    print_msg(f'Event: {event}')
    print_msg(f'Context: {context}')
    event_body = loads(event['body'])
    parent_tag_record_id = event_body['parentTagRecordId']
    team_id = int(event_body['parentTagTeamId'])
    total_count = event_body['cardsRequired']
    airtable_base_id = event_body['baseId']
    questions_table_id = event_body['tableId']
    tags_table_id = event_body['tagsTableId']
    out = (
        parent_tag_record_id, team_id, total_count, airtable_base_id,
        questions_table_id, tags_table_id
    )
    return out


def __get_not_used_questions_from_rds(parent_tag_record_id):
    connection, cursor = setup_connection_to_templates_db()
    questions = select_query(
        cursor,
        'question, tags, parent_tags, difficulty, '
        'template, answer, insert_time, in_use',
        'templates',
        f'in_use = 0 and parent_tags like \'%{parent_tag_record_id}%\'',
        'templates',
    )
    close_connection_to_templates_db(connection, cursor)
    return questions


def __generate_new_questions_to_use_and_add_to_rds(
        parent_tag_record_id, team_id, count, base_id, table_id
):
    new_questions = __generate_new_questions(
        parent_tag_record_id, team_id, 3 * count, base_id, table_id
    )
    new_questions_to_use = new_questions[:count]
    new_questions_not_used = new_questions[count:]
    __add_new_questions_to_rds(new_questions_to_use, new_questions_not_used)
    return new_questions_to_use


def __generate_new_questions(
        parent_tag_record_id, team_id, count, base_id, table_id
):
    questions = run_industrial_generation(team_id, count)
    questions = __replace_tags_and_parent_tags_with_corresponding_records_ids(
        parent_tag_record_id, base_id, table_id, questions
    )
    questions = __flatten_questions(questions)
    questions = __exclude_duplicates(questions)
    np.random.shuffle(questions)
    return questions


def __replace_tags_and_parent_tags_with_corresponding_records_ids(
        parent_tag_record_id, base_id, table_id, questions
):
    questions = __replace_parent_tags_with_record_id(
        parent_tag_record_id, questions
    )
    questions = __replace_tags_with_records_ids(base_id, table_id, questions)
    return questions


def __replace_parent_tags_with_record_id(parent_tag_record_id, questions):
    for single_question in questions.keys():
        questions[single_question]['parent_tags'] = [parent_tag_record_id]
    return questions


def __replace_tags_with_records_ids(base_id, table_id, questions):
    airtable_cursor = airtable.Airtable(base_id, AIRTABLE_API_KEY)
    existing_tags = __get_existing_tags(airtable_cursor, table_id)
    for single_question in tqdm(list(questions.keys())):
        tags_records_ids = []
        for single_tag in questions[single_question]['tags']:
            single_record_id = existing_tags.get(
                __preprocess_tag(single_tag), None
            )
            if single_record_id is None:
                single_record_id = __add_new_tag_to_airtable(
                    airtable_cursor, table_id, single_tag
                )
                existing_tags[__preprocess_tag(single_tag)] = single_record_id
            tags_records_ids.append(single_record_id)
        tags_records_ids = list(set(tags_records_ids))
        questions[single_question]['tags'] = tags_records_ids
    return questions


def __preprocess_tag(tag):
    tag = tag.lower()
    tag = tag.strip()
    tag = normalize('NFKD', tag).encode('ascii', 'ignore').decode('ascii')
    return tag


def __get_existing_tags(airtable_cursor, table_id):
    tags_table = __get_all_airtable_records(airtable_cursor, table_id)
    existing_tags = {}
    for single_entry in tags_table['records']:
        tag_name = single_entry['fields']['Tag']
        preprocessed_tag_name = __preprocess_tag(tag_name)
        tag_record_id = single_entry['fields']['Record_ID']
        existing_tags[preprocessed_tag_name] = tag_record_id
    return existing_tags


def __get_all_airtable_records(airtable_cursor, table_id):
    records = {'records': []}
    for single_record in airtable_cursor.iterate(table_id):
        records['records'].append(single_record)
    return records


def __add_new_tag_to_airtable(airtable_cursor, table_id, single_tag):
    new_tag_data = {'Tag': single_tag, 'Status': 'Requested'}
    new_entry = airtable_cursor.create(table_id, new_tag_data)
    new_record_id = new_entry['fields']['Record_ID']
    return new_record_id


def __flatten_questions(questions):
    out = []
    for single_question in questions.keys():
        single_entry = (
            single_question,
            f'{questions[single_question]["tags"]}',
            f'{questions[single_question]["parent_tags"]}',
            questions[single_question]['difficulty'],
            questions[single_question]['template'],
            int(questions[single_question]['answer']),
            '',
            ''
        )
        out.append(single_entry)
    return out


def __exclude_duplicates(questions):
    all_existing_questions = __get_all_existing_questions()
    q_set = {q[0] for q in all_existing_questions}
    questions = [q for q in questions if q[0] not in q_set]
    return questions


def __get_all_existing_questions():
    connection, cursor = setup_connection_to_templates_db()
    questions = select_query(
        cursor,
        'question, tags, parent_tags, difficulty, '
        'template, answer, insert_time, in_use',
        'templates',
        db_name='templates',
    )
    close_connection_to_templates_db(connection, cursor)
    return questions


def __add_new_questions_to_rds(used, not_used):
    connection, cursor = setup_connection_to_templates_db()
    for single_entry in used:
        __add_new_single_entry_to_rds(cursor, single_entry, True)
    for single_entry in not_used:
        __add_new_single_entry_to_rds(cursor, single_entry, False)
    connection.commit()
    close_connection_to_templates_db(connection, cursor)


def __add_new_single_entry_to_rds(cursor, single_entry, in_use):
    columns = (
        'question', 'tags', 'parent_tags', 'difficulty',
        'template', 'answer', 'insert_time', 'in_use'
    )
    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    single_entry = single_entry[:6] + (timestamp, int(in_use))
    insert_into_query(cursor, 'templates', columns, single_entry, 'templates')


def __update_questions_as_used(questions):
    connection, cursor = setup_connection_to_templates_db()
    columns = (
        'question', 'tags', 'parent_tags', 'difficulty',
        'template', 'answer', 'insert_time', 'in_use'
    )
    for single_entry in questions:
        q, tags, parent_tags, d, template, a, insert_time, _ = single_entry
        insert_time = insert_time.strftime('%Y-%m-%d %H:%M:%S')
        update_query(
            cursor, 'templates', columns,
            (q, tags, parent_tags, d, template, a, insert_time, int(True)),
            f"question = {q.__repr__()}",
            'templates'
        )
    connection.commit()
    close_connection_to_templates_db(connection, cursor)


def __send_questions_to_airtable(base_id, table_id, questions):
    airtable_cursor = airtable.Airtable(base_id, AIRTABLE_API_KEY)
    sent_count = 0
    for single_entry in tqdm(questions):
        q, tags, parent_tags, difficulty, _, answer, _, _ = single_entry
        new_question_data = {
            'Card': q,
            'Tags': ast.literal_eval(tags),
            'Parent-tag': ast.literal_eval(parent_tags),
            'Tier': difficulty.capitalize(),
            'Answer': 'TRUE' if answer == 1 else 'FALSE',

        }
        success = False
        efforts_count = 0
        while not success and efforts_count < 10:
            success = __try_to_send_single_question(
                airtable_cursor, table_id, new_question_data
            )
            efforts_count += 1
        if success:
            sent_count += 1
    return sent_count


def __try_to_send_single_question(airtable_cursor, table_id, question_data):
    try:
        airtable_cursor.create(table_id, question_data)
        return True
    except AirtableError:
        return False
    except Exception as e:
        raise e
