import random
from typing import Dict
from app import run_comfy_workflow_and_send_image

user_sessions: Dict[str, Dict] = {}

question_pool = [
    {
        "question": [
            "You wake up in a magical land. What's the first thing you do?",
            "You step into a new city. What do you look for first?",
            "A door appears out of nowhere. What do you hope is behind it?"
        ],
        "options": [
            ("Join a team to play", "Hero"),
            ("Break rules, ride free", "Outlaw"),
            ("Talk to someone nearby", "Lover"),
            ("Explore every corner", "Explorer")
        ]
    },
    {
        "question": [
            "What outfit do you wear in your fantasy world?",
            "Choose your dream look:",
            "Your style in another universe is..."
        ],
        "options": [
            ("Armor or sports outfit", "Hero"),
            ("Dark, spiked outfit", "Outlaw"),
            ("Soft, romantic colors", "Lover"),
            ("Boots and travel bag", "Explorer")
        ]
    },
    {
        "question": [
            "A gift box appears. What's inside?",
            "You find a box on the road. It contains...",
            "You open a chest. It holds..."
        ],
        "options": [
            ("Trophy or guitar pick", "Hero"),
            ("Spiked bracelet", "Outlaw"),
            ("Love letter", "Lover"),
            ("Map or space helmet", "Explorer")
        ]
    }
]

final_profile_question = {
    "Hero": [("You are in a stadium. What are you doing?", [
        ("Scoring a goal", "The Golden Goal Seeker"),
        ("Raising your sword", "The Iron Gladiator"),
        ("Runway walk", "The Style Iconoclast"),
        ("On stage with guitar", "The Stage Stormer")
    ])],
    "Outlaw": [("Pick your rebel moment:", [
        ("Ride into sunset", "The Wanted and Wild"),
        ("Night Crawling", "The Shadow Striker")
    ])],
    "Lover": [("What feels most romantic to you?", [
        ("Train ride with love", "The Hopeless Romantic"),
        ("Rain + Cricket", "The Strike Master")
    ])],
    "Explorer": [("Choose your adventure:", [
        ("Fly to the stars", "The Starbound Voyager"),
        ("Ride desert trails", "The Midnight Rider")
    ])]
}

def reset_session(user_id: str):
    user_sessions[user_id] = {
        "archetypes": {"Hero": 0, "Outlaw": 0, "Lover": 0, "Explorer": 0},
        "question_index": 0,
        "final_stage": False
    }

def update_user_session(user_id: str, key: str, value):
    if user_id not in user_sessions:
        reset_session(user_id)
    user_sessions[user_id][key] = value

def get_next_question(user_id: str) -> Dict:
    session = user_sessions.get(user_id, {})
    if "name" not in session:
        return {"type": "text", "text": "What's your name?"}

    if "gender" not in session:
        return {
            "type": "buttons",
            "text": "What is your gender?",
            "buttons": [
                {"id": "M", "title": "Male"},
                {"id": "F", "title": "Female"},
                
            ]
        }

    index = session.get("question_index", 0)
    if index < len(question_pool):
        pool = question_pool[index]
        session["current_archetypes"] = pool["options"]
        q_text = random.choice(pool["question"])
        return {
            "type": "list",
            "text": q_text,
            "buttons": [
                {"id": str(i), "title": opt[0][:24]} for i, opt in enumerate(pool["options"])
            ]
        }

    if session.get("final_stage", False) and "final_profile" not in session:

        top = max(session["archetypes"], key=session["archetypes"].get)
        session["final_archetype"] = top
        session["final_stage"] = True
        q_text, options = random.choice(final_profile_question[top])
        session["final_options"] = options
        return {
            "type": "list",
            "text": q_text,
            "buttons": [
                {"id": str(i), "title": opt[0][:24]} for i, opt in enumerate(options)
            ]
        }

    return {"type": "done"}


def handle_response(user_id: str, payload: str):
    session = user_sessions[user_id]

    if session.get("final_stage", False):
        final_opts = session.get("final_options")
        if final_opts:
            idx = int(payload)
            session["final_profile"] = final_opts[idx][1]
            print(f"✅ Profile selected: {session['final_profile']}")


        else:
            print("⚠️ Final options missing, cannot proceed.")
        return

    idx = int(payload)
    index = session["question_index"]
    archetype = session["current_archetypes"][idx][1]
    session["archetypes"][archetype] += 1
    session["question_index"] += 1

    if session["question_index"] >= len(question_pool):
        top = max(session["archetypes"], key=session["archetypes"].get)
        session["final_archetype"] = top
        session["final_stage"] = True
        q_text, options = random.choice(final_profile_question[top])
        session["final_options"] = options
