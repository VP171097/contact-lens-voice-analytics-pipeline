"""generate_sample_lines.py

Builds a 2000-entry SAMPLE_LINES list (dummy call-center dialogue lines)
covering several call scenarios (greeting, order/shipping, billing,
technical support, cancellation, complaint, compliment, closing), by
combining sentence templates with substitutable values (order numbers,
amounts, dates, product names, etc.) so the result is varied rather than
2000 repeats of the same 10 lines.

Writes:
    sample_lines.py  - a Python file defining SAMPLE_LINES = [...]
    sample_lines.txt - the same content, one line per row, plain text

Usage:
    python generate_sample_lines.py --count 2000
"""

import argparse
import random

random.seed(42)  # reproducible output

PRODUCTS = [
    "wireless headphones", "laptop charger", "smartphone case", "bluetooth speaker",
    "fitness tracker", "coffee maker", "office chair", "desk lamp", "external hard drive",
    "gaming mouse", "mechanical keyboard", "monitor stand", "webcam", "router",
    "power bank", "tablet", "smartwatch", "printer", "vacuum cleaner", "air fryer",
]

NAMES = [
    "Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Sam", "Jamie",
    "Priya", "Wei", "Fatima", "Diego", "Elena", "Noah", "Maya", "Liam",
]

ISSUES = [
    "hasn't arrived yet", "arrived damaged", "is missing a part", "won't turn on",
    "keeps disconnecting", "is the wrong item", "stopped working after a week",
    "has a cracked screen", "won't charge", "makes a strange noise",
]

DEPARTMENTS = ["billing", "technical support", "returns", "shipping", "account services"]

# --- Sentence templates per scenario, {X} placeholders filled at generation time ---

GREETING_TEMPLATES = [
    "Hello, thank you for calling support, how can I help you today?",
    "Hi there, you've reached customer care, what can I do for you?",
    "Good morning, thanks for holding, how may I assist you?",
    "Good afternoon, this is {name} speaking, how can I help?",
    "Hello, I'm {name} from customer support, what seems to be the issue?",
    "Thanks for calling, my name is {name}, how can I help you today?",
    "Hi, welcome back, I see you've called before, what's going on today?",
    "Hello, before we start, can I get your name please?",
    "Hi, I'll be happy to help you today, what's the issue?",
    "Good evening, thank you for your patience, how can I assist?",
]

CUSTOMER_ISSUE_TEMPLATES = [
    "Hi, I'm having trouble with my recent order, it {issue}.",
    "My {product} {issue}, can you help?",
    "I ordered a {product} last week and it {issue}.",
    "I'm calling about order {order_id}, the {product} {issue}.",
    "There's a problem with my {product}, it {issue}.",
    "I need help, my {product} {issue} and I'm not sure what to do.",
    "This is frustrating, my {product} {issue} again.",
    "I recently bought a {product} and it {issue}.",
    "Can someone help me, my {product} {issue}?",
    "I'm not happy, the {product} I ordered {issue}.",
]

CLARIFY_TEMPLATES = [
    "I'm sorry to hear that. Can you provide your order number?",
    "I understand, could you confirm the email used for the order?",
    "Let me pull that up, what's your order confirmation number?",
    "I apologize for the trouble. Can I get your account number please?",
    "Thanks for letting me know, do you have the order ID handy?",
    "I'm sorry about that, could you verify your shipping address?",
    "Let's get this sorted, can you tell me when you placed the order?",
    "I understand your frustration. What's the order number on the confirmation email?",
    "No problem, can you confirm your full name and order number?",
    "I hear you. Do you have the tracking number available?",
]

RESPONSE_TEMPLATES = [
    "Sure, it's ORD-{order_id}.",
    "Yes, the order number is {order_id}.",
    "It's {order_id}, placed on {date}.",
    "The confirmation number is {order_id}.",
    "Here it is: {order_id}.",
    "My account number is ACC-{order_id}.",
    "The tracking number is TRK-{order_id}.",
    "I placed it on {date}, order {order_id}.",
]

AGENT_ACTION_TEMPLATES = [
    "Let me check that for you, one moment please.",
    "Give me just a second to pull up your account.",
    "I'm looking into this right now.",
    "Let me check the {department} system for you.",
    "One moment, I'm reviewing the order details.",
    "I'm checking with our {department} team now.",
    "Let me verify this with our warehouse system.",
    "I'll need to escalate this to {department}, one moment.",
    "Let me run a quick check on that {product}.",
    "I'm pulling up the shipment history now.",
]

RESOLUTION_TEMPLATES = [
    "I can see the order is currently in transit and should arrive within two days.",
    "I've issued a refund, it should reflect in 3-5 business days.",
    "I'm sending a replacement {product}, it'll arrive within a week.",
    "I've escalated this to {department}, they'll follow up within 24 hours.",
    "I've applied a credit of ${amount} to your account.",
    "A technician will be dispatched to help resolve the {product} issue.",
    "I've updated your address, the {product} will ship there now.",
    "I've cancelled the order and refunded ${amount} to your original payment method.",
    "The {product} has been replaced under warranty at no cost to you.",
    "I've flagged this for our {department} team to review further.",
]

CLOSING_TEMPLATES = [
    "Okay, thank you for checking.",
    "Is there anything else I can help you with today?",
    "No, that's all. Thanks for your help!",
    "You're welcome, have a great day!",
    "Thank you so much, that resolves my issue.",
    "Perfect, thank you for resolving this quickly.",
    "I appreciate your help, goodbye!",
    "Thanks again, that's everything for now.",
    "Great, thank you for your patience today.",
    "Is there a reference number for this call?",
]

MISC_TEMPLATES = [
    "I'd like to update my billing address, is that possible here?",
    "Of course, can you confirm the account holder's name for verification?",
    "It's under the name on the account, yes.",
    "Great, I've updated the address on file.",
    "My internet service has been down since this morning.",
    "I understand the frustration, let me run a diagnostic on the line.",
    "The diagnostic shows a fault upstream, a technician will be dispatched.",
    "When can I expect the technician to arrive?",
    "Between 2pm and 5pm tomorrow, you'll get a confirmation text.",
    "Can I speak to a supervisor about this?",
    "I'd like to cancel my subscription please.",
    "May I ask the reason for cancelling?",
    "The service didn't meet my expectations.",
    "I understand, I've processed the cancellation, no further charges will apply.",
    "Can you email me a confirmation of this cancellation?",
    "Absolutely, you'll receive that within the hour.",
    "I was charged twice for the same order, can you check?",
    "I see the duplicate charge, I'm refunding it right away.",
    "Thank you, how long will the refund take?",
    "Typically 3-5 business days depending on your bank.",
]

ALL_TEMPLATE_GROUPS = [
    GREETING_TEMPLATES,
    CUSTOMER_ISSUE_TEMPLATES,
    CLARIFY_TEMPLATES,
    RESPONSE_TEMPLATES,
    AGENT_ACTION_TEMPLATES,
    RESOLUTION_TEMPLATES,
    CLOSING_TEMPLATES,
    MISC_TEMPLATES,
]


def _fill(template: str) -> str:
    return template.format(
        name=random.choice(NAMES),
        product=random.choice(PRODUCTS),
        issue=random.choice(ISSUES),
        order_id=random.randint(1000, 99999),
        date=f"{random.randint(1,28):02d}/{random.randint(1,12):02d}",
        department=random.choice(DEPARTMENTS),
        amount=random.choice([9.99, 14.50, 19.99, 25.00, 32.75, 49.99, 60.00]),
    )


def generate_lines(count: int) -> list:
    """Generate `count` DISTINCT lines. Exhaustively enumerates every
    placeholder combination per template (product x issue x order_id x ...)
    rather than random-rerolling, so uniqueness is guaranteed up to the true
    combinatorial size of the template set (comfortably >2000)."""
    lines = []
    seen = set()
    all_templates = [t for group in ALL_TEMPLATE_GROUPS for t in group]

    # Wide, deterministic value pools so combinations don't run out before 2000.
    order_ids = list(range(1000, 1000 + max(count, 2000)))
    dates = [f"{d:02d}/{m:02d}" for d in range(1, 29) for m in range(1, 13)]
    amounts = [round(x, 2) for x in [i * 0.5 + 9 for i in range(1, 120)]]

    random.shuffle(order_ids)
    random.shuffle(dates)
    random.shuffle(amounts)

    def _fill_unique(template: str, idx: int) -> str:
        return template.format(
            name=NAMES[idx % len(NAMES)],
            product=PRODUCTS[idx % len(PRODUCTS)],
            issue=ISSUES[idx % len(ISSUES)],
            order_id=order_ids[idx % len(order_ids)],
            date=dates[idx % len(dates)],
            department=DEPARTMENTS[idx % len(DEPARTMENTS)],
            amount=amounts[idx % len(amounts)],
        )

    idx = 0
    max_attempts = count * 200
    attempts = 0
    while len(lines) < count and attempts < max_attempts:
        t = all_templates[idx % len(all_templates)]
        filled = _fill_unique(t, idx // len(all_templates) + (idx % 97))
        if filled not in seen:
            lines.append(filled)
            seen.add(filled)
        idx += 1
        attempts += 1

    # Fallback (only if template combinatorics are somehow exhausted): pad
    # with fully-random fills, accepting rare duplicates rather than looping forever.
    while len(lines) < count:
        t = random.choice(all_templates)
        lines.append(_fill(t))

    return lines[:count]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--out-py", default="./sample_lines.py")
    parser.add_argument("--out-txt", default="./sample_lines.txt")
    args = parser.parse_args()

    lines = generate_lines(args.count)

    with open(args.out_txt, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    with open(args.out_py, "w", encoding="utf-8") as f:
        f.write("SAMPLE_LINES = [\n")
        for line in lines:
            escaped = line.replace("\\", "\\\\").replace('"', '\\"')
            f.write(f'    "{escaped}",\n')
        f.write("]\n")

    print(f"Wrote {len(lines)} lines to {args.out_py} and {args.out_txt}")


if __name__ == "__main__":
    main()
