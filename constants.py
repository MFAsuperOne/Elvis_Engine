EASY = 'easy'
MEDIUM = 'medium'
HARD = 'hard'

MAIN_POSITIONS = (
    'defender',
    'midfielder',
    'attacker',
    'goalkeeper',
)
SPECIFIC_POSITIONS = (
    'centre-back',
    'right-back',
    'left-back',
    'wing-back',

    'defensive midfield',
    'central midfield',
    'attacking midfield',
    'right midfield',
    'left midfield',

    'centre-forward',
    'striker',
    'left winger',
    'right winger',

    'goalkeeper',
)
SPECIFIC_TO_MAIN_MAPPING = {
    'centre-back': 'defender',
    'right-back': 'defender',
    'left-back': 'defender',
    'wing-back': 'defender',

    'defensive midfield': 'midfielder',
    'central midfield': 'midfielder',
    'attacking midfield': 'midfielder',
    'right midfield': 'midfielder',
    'left midfield': 'midfielder',

    'centre-forward': 'attacker',
    'striker': 'attacker',
    'left winger': 'attacker',
    'right winger': 'attacker',

    'goalkeeper': 'goalkeeper',
}
VOWELS = ('a', 'e', 'i', 'o', 'u')
TRANSFER_INVALID_TEAMS = ('retired', 'without club', 'unknown')
