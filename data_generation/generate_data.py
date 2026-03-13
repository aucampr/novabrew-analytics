"""
NovaBrew Coffee — Synthetic Data Generator
==========================================
Post 1: Building the Data Foundation

Generates 18 months of realistic D2C e-commerce data across 5 tables:
  - customers
  - orders
  - order_items
  - sessions
  - ad_spend
  - email_events

Output: CSV files ready for BigQuery upload (./output/)

Usage:
    python generate_data.py

Requirements:
    pip install pandas numpy
"""

import pandas as pd
import numpy as np
import random
import os
import json
from datetime import datetime, timedelta

# ── Config ────────────────────────────────────────────────────────────────────

SEED = 42
np.random.seed(SEED)
random.seed(SEED)

START_DATE = datetime(2023, 1, 1)
END_DATE   = datetime(2024, 6, 30)
OUTPUT_DIR = "./output"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Reference Data ────────────────────────────────────────────────────────────

PRODUCTS = [
    {"product_id": "P001", "name": "Morning Ritual Blend",   "category": "whole_bean", "price": 18.99, "cogs": 6.50},
    {"product_id": "P002", "name": "Dark Roast Espresso",    "category": "whole_bean", "price": 21.99, "cogs": 7.20},
    {"product_id": "P003", "name": "Single Origin Ethiopia", "category": "whole_bean", "price": 26.99, "cogs": 9.80},
    {"product_id": "P004", "name": "Cold Brew Starter Kit",  "category": "equipment",  "price": 34.99, "cogs": 12.00},
    {"product_id": "P005", "name": "Subscription Box — 2 bags/mo", "category": "subscription", "price": 38.99, "cogs": 13.50},
    {"product_id": "P006", "name": "Decaf House Blend",      "category": "whole_bean", "price": 17.99, "cogs": 6.20},
    {"product_id": "P007", "name": "Nitro Cold Brew Cans 6pk","category": "ready_to_drink", "price": 22.99, "cogs": 8.40},
    {"product_id": "P008", "name": "Reusable Brew Bag",      "category": "equipment",  "price": 12.99, "cogs": 3.20},
]

CHANNELS = ["organic_search", "paid_search", "paid_social", "email", "direct", "referral", "organic_social"]
CHANNEL_WEIGHTS = [0.22, 0.18, 0.20, 0.15, 0.12, 0.07, 0.06]

AD_CAMPAIGNS = {
    "paid_search": [
        {"id": "GS001", "name": "Brand Keywords"},
        {"id": "GS002", "name": "Coffee Subscription"},
        {"id": "GS003", "name": "Competitor Conquesting"},
    ],
    "paid_social": [
        {"id": "FB001", "name": "Prospecting — Interest"},
        {"id": "FB002", "name": "Retargeting — Site Visitors"},
        {"id": "FB003", "name": "LAL — Top Customers"},
        {"id": "IG001", "name": "Instagram — Story Ads"},
    ],
}

US_STATES = [
    "CA","TX","NY","FL","WA","CO","IL","MA","OR","GA",
    "AZ","NC","VA","PA","OH","TN","MN","MI","NJ","NV"
]
STATE_WEIGHTS = [0.15,0.11,0.10,0.08,0.06,0.06,0.05,0.05,0.04,0.04,
                 0.03,0.03,0.03,0.03,0.03,0.03,0.02,0.02,0.02,0.02]

EMAIL_FLOWS = [
    "welcome_series",
    "post_purchase",
    "abandoned_cart",
    "win_back",
    "promotional_blast",
    "subscription_renewal",
]

FIRST_NAMES = [
    "James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda",
    "William","Barbara","David","Susan","Richard","Jessica","Joseph","Sarah",
    "Thomas","Karen","Charles","Lisa","Christopher","Nancy","Daniel","Betty",
    "Matthew","Margaret","Anthony","Sandra","Mark","Ashley","Donald","Dorothy",
    "Steven","Kimberly","Paul","Emily","Andrew","Donna","Joshua","Michelle",
    "Kenneth","Carol","Kevin","Amanda","Brian","Melissa","George","Deborah",
    "Timothy","Stephanie","Ronald","Rebecca","Edward","Sharon","Jason","Laura",
    "Jeffrey","Cynthia","Ryan","Kathleen","Jacob","Amy","Gary","Angela",
    "Nicholas","Shirley","Eric","Anna","Jonathan","Brenda","Stephen","Emma",
    "Larry","Pamela","Justin","Virginia","Scott","Katherine","Brandon","Christine",
    "Benjamin","Samantha","Samuel","Debra","Raymond","Rachel","Gregory","Janet",
    "Frank","Carolyn","Alexander","Catherine","Patrick","Maria","Jack","Heather",
]

LAST_NAMES = [
    "Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis",
    "Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson",
    "Thomas","Taylor","Moore","Jackson","Martin","Lee","Perez","Thompson","White",
    "Harris","Sanchez","Clark","Ramirez","Lewis","Robinson","Walker","Young",
    "Allen","King","Wright","Scott","Torres","Nguyen","Hill","Flores","Green",
    "Adams","Nelson","Baker","Hall","Rivera","Campbell","Mitchell","Carter","Roberts",
]

# ── Helper Functions ──────────────────────────────────────────────────────────

def date_range_list(start, end):
    """Return list of all dates between start and end."""
    days = (end - start).days + 1
    return [start + timedelta(days=i) for i in range(days)]

def seasonality_multiplier(date):
    """
    Apply realistic seasonal demand patterns for a D2C coffee brand.
    - Q4 (Oct–Dec): Black Friday, holiday gifting — peak
    - Jan: New Year resolutions boost
    - Summer (Jun–Aug): slight dip, cold brew uptick
    - Feb: Valentine's Day spike
    """
    month = date.month
    day   = date.day

    base = {
        1: 1.15,   # New Year boost
        2: 1.05,   # Valentine's Day
        3: 0.95,
        4: 0.95,
        5: 1.00,
        6: 0.90,   # Summer dip
        7: 0.85,
        8: 0.88,
        9: 1.00,   # Back to office
        10: 1.10,  # Pre-holiday ramp
        11: 1.45,  # Black Friday / Cyber Monday
        12: 1.35,  # Holiday gifting
    }.get(month, 1.0)

    # Weekend uplift (Fri=5, Sat=6, Sun=0 in weekday())
    dow = date.weekday()
    weekend_boost = 1.12 if dow in [5, 6] else (1.05 if dow == 4 else 1.0)

    # Black Friday spike (last Friday of November)
    if month == 11 and dow == 4 and day >= 22:
        return base * weekend_boost * 2.8

    # Cyber Monday
    if month == 11 and dow == 0 and day >= 25:
        return base * weekend_boost * 2.2

    return base * weekend_boost

def growth_multiplier(date):
    """Linear brand growth over 18 months: starts at 1.0, ends at ~2.4x."""
    total_days = (END_DATE - START_DATE).days
    elapsed    = (date - START_DATE).days
    return 1.0 + (1.4 * elapsed / total_days)


# ── Table 1: Customers ────────────────────────────────────────────────────────

def generate_customers(n=8000):
    print(f"  Generating {n} customers...")
    customers = []
    dates = date_range_list(START_DATE, END_DATE)

    for i in range(1, n + 1):
        signup_date = random.choice(dates)
        first = random.choice(FIRST_NAMES)
        last  = random.choice(LAST_NAMES)
        email = f"{first.lower()}.{last.lower()}{random.randint(1,999)}@{random.choice(['gmail.com','yahoo.com','outlook.com','icloud.com'])}"

        acq_channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]

        # Subscription customers skew toward direct / email
        is_subscriber = random.random() < 0.18
        if is_subscriber:
            acq_channel = random.choices(["email","paid_social","organic_search"], weights=[0.3,0.4,0.3])[0]

        customers.append({
            "customer_id":      f"C{i:06d}",
            "first_name":       first,
            "last_name":        last,
            "email":            email,
            "state":            random.choices(US_STATES, weights=STATE_WEIGHTS)[0],
            "acquisition_channel": acq_channel,
            "signup_date":      signup_date.strftime("%Y-%m-%d"),
            "is_subscriber":    is_subscriber,
            "email_opt_in":     random.random() < 0.74,
            "date_of_birth":    (signup_date - timedelta(days=random.randint(365*22, 365*55))).strftime("%Y-%m-%d"),
        })

    return pd.DataFrame(customers)


# ── Table 2: Orders & Order Items ─────────────────────────────────────────────

def generate_orders(customers_df):
    print("  Generating orders and order items...")
    orders      = []
    order_items = []
    order_id    = 1
    item_id     = 1

    products_map = {p["product_id"]: p for p in PRODUCTS}

    for _, cust in customers_df.iterrows():
        signup = datetime.strptime(cust["signup_date"], "%Y-%m-%d")

        # Number of orders — power-law distribution (most buy 1-2x, some are loyalists)
        n_orders = int(np.random.pareto(1.8) + 1)
        n_orders = min(n_orders, 24)  # cap at 24 orders over 18 months

        prev_order_date = signup

        for o in range(n_orders):
            # Space orders out realistically
            days_gap = max(1, int(np.random.exponential(35)))
            order_date = prev_order_date + timedelta(days=days_gap)

            if order_date > END_DATE:
                break

            # Apply seasonality to whether order happens
            season_mult = seasonality_multiplier(order_date)
            if random.random() > (0.7 * season_mult):
                continue

            prev_order_date = order_date

            # Channel — repeat customers skew toward email & direct
            if o == 0:
                channel = cust["acquisition_channel"]
            else:
                channel = random.choices(
                    ["email","direct","organic_search","paid_search","paid_social","organic_social"],
                    weights=[0.28,0.25,0.20,0.12,0.10,0.05]
                )[0]

            # Promo / discount
            has_promo = random.random() < (0.35 if o == 0 else 0.20)
            discount_pct = random.choice([0.10, 0.15, 0.20]) if has_promo else 0.0
            promo_code = random.choice(["WELCOME10","SUMMER15","BFCM20","LOYAL15",""] ) if has_promo else None

            # Items in this order
            n_items = random.choices([1, 2, 3, 4], weights=[0.55, 0.28, 0.12, 0.05])[0]
            chosen_products = random.choices(PRODUCTS, k=n_items)

            subtotal = 0
            for prod in chosen_products:
                qty   = random.choices([1, 2, 3], weights=[0.70, 0.22, 0.08])[0]
                price = prod["price"]
                line_total = round(price * qty * (1 - discount_pct), 2)
                subtotal  += line_total

                order_items.append({
                    "item_id":       f"OI{item_id:08d}",
                    "order_id":      f"O{order_id:08d}",
                    "customer_id":   cust["customer_id"],
                    "product_id":    prod["product_id"],
                    "product_name":  prod["name"],
                    "category":      prod["category"],
                    "quantity":      qty,
                    "unit_price":    price,
                    "discount_pct":  discount_pct,
                    "line_total":    line_total,
                    "cogs":          round(prod["cogs"] * qty, 2),
                })
                item_id += 1

            shipping = 0 if subtotal >= 45 else 5.99
            tax      = round(subtotal * 0.08, 2)
            total    = round(subtotal + shipping + tax, 2)

            # Fulfillment
            fulfillment_days = random.choices([1,2,3,4,5], weights=[0.10,0.35,0.30,0.15,0.10])[0]
            shipped_date     = order_date + timedelta(days=1)
            delivered_date   = order_date + timedelta(days=fulfillment_days + 1)

            status = "delivered"
            if random.random() < 0.015:
                status = "refunded"
            elif random.random() < 0.005:
                status = "cancelled"

            orders.append({
                "order_id":        f"O{order_id:08d}",
                "customer_id":     cust["customer_id"],
                "order_date":      order_date.strftime("%Y-%m-%d"),
                "shipped_date":    shipped_date.strftime("%Y-%m-%d"),
                "delivered_date":  delivered_date.strftime("%Y-%m-%d"),
                "channel":         channel,
                "promo_code":      promo_code,
                "discount_pct":    discount_pct,
                "subtotal":        round(subtotal, 2),
                "shipping_cost":   shipping,
                "tax":             tax,
                "order_total":     total,
                "status":          status,
                "is_first_order":  (o == 0),
            })
            order_id += 1

    return pd.DataFrame(orders), pd.DataFrame(order_items)


# ── Table 3: Sessions ─────────────────────────────────────────────────────────

def generate_sessions(orders_df, n_sessions=180000):
    print(f"  Generating ~{n_sessions:,} website sessions...")
    sessions  = []
    dates     = date_range_list(START_DATE, END_DATE)

    # Build a lookup of orders by date for conversion simulation
    orders_by_date = orders_df.groupby("order_date")["order_id"].apply(list).to_dict()

    pages = ["/", "/shop", "/product/morning-ritual", "/product/dark-roast",
             "/product/ethiopia", "/product/cold-brew-kit", "/product/subscription",
             "/about", "/blog", "/cart", "/checkout"]

    session_id = 1

    for date in dates:
        season_mult = seasonality_multiplier(date)
        growth_mult = growth_multiplier(date)
        daily_sessions = int(np.random.normal(
            loc=n_sessions / len(dates) * season_mult * growth_mult,
            scale=50
        ))
        daily_sessions = max(20, daily_sessions)

        for _ in range(daily_sessions):
            channel     = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS)[0]
            device      = random.choices(["mobile","desktop","tablet"], weights=[0.58,0.35,0.07])[0]
            bounced     = random.random() < (0.52 if device == "mobile" else 0.38)
            pages_viewed = 1 if bounced else random.choices([2,3,4,5,6,7], weights=[0.25,0.25,0.20,0.15,0.10,0.05])[0]
            duration_s  = 8 if bounced else int(np.random.exponential(180) + 30)

            # Landing page varies by channel
            if channel == "paid_search":
                landing = random.choices(["/shop","/product/subscription","/product/morning-ritual"], weights=[0.4,0.35,0.25])[0]
            elif channel == "paid_social":
                landing = random.choices(["/product/subscription","/product/cold-brew-kit","/"], weights=[0.45,0.30,0.25])[0]
            elif channel == "email":
                landing = random.choices(["/product/morning-ritual","/shop","/product/subscription"], weights=[0.40,0.35,0.25])[0]
            else:
                landing = random.choices(pages, weights=[0.30,0.15,0.10,0.08,0.07,0.05,0.08,0.05,0.05,0.04,0.03])[0]

            # Assign UTM params
            utm_source   = None
            utm_medium   = None
            utm_campaign = None

            if channel == "paid_search":
                utm_source, utm_medium = "google", "cpc"
                utm_campaign = random.choice(["brand-keywords","coffee-subscription","competitor"])
            elif channel == "paid_social":
                utm_source = random.choice(["facebook","instagram"])
                utm_medium = "paid_social"
                utm_campaign = random.choice(["prospecting-interest","retargeting-site","lal-top-customers"])
            elif channel == "email":
                utm_source, utm_medium = "klaviyo", "email"
                utm_campaign = random.choice(EMAIL_FLOWS)
            elif channel == "organic_social":
                utm_source, utm_medium = random.choice(["instagram","tiktok"]), "social"

            sessions.append({
                "session_id":    f"S{session_id:010d}",
                "session_date":  date.strftime("%Y-%m-%d"),
                "channel":       channel,
                "device":        device,
                "landing_page":  landing,
                "pages_viewed":  pages_viewed,
                "duration_seconds": duration_s,
                "bounced":       bounced,
                "utm_source":    utm_source,
                "utm_medium":    utm_medium,
                "utm_campaign":  utm_campaign,
                "converted":     False,  # will update below
                "order_id":      None,
            })
            session_id += 1

        # Tag converting sessions (match ~real conversion rate ~2.8%)
        date_str = date.strftime("%Y-%m-%d")
        if date_str in orders_by_date:
            n_conversions = len(orders_by_date[date_str])
            day_sessions  = [s for s in sessions if s["session_date"] == date_str]
            if day_sessions and n_conversions > 0:
                convert_sample = random.sample(day_sessions, min(n_conversions, len(day_sessions)))
                orders_list    = orders_by_date[date_str]
                for idx, sess in enumerate(convert_sample):
                    sess["converted"] = True
                    sess["order_id"]  = orders_list[idx] if idx < len(orders_list) else None

    return pd.DataFrame(sessions)


# ── Table 4: Ad Spend ─────────────────────────────────────────────────────────

def generate_ad_spend():
    print("  Generating ad spend data...")
    rows  = []
    dates = date_range_list(START_DATE, END_DATE)

    # Monthly budget ramp: starts low, grows as brand scales
    def monthly_budget(date):
        months_in = (date.year - START_DATE.year) * 12 + (date.month - START_DATE.month)
        return 3000 + (months_in * 400)  # $3k/mo → ~$9k/mo by month 18

    for date in dates:
        season = seasonality_multiplier(date)
        daily_budget = monthly_budget(date) / 30

        for platform in ["google_ads", "meta_ads"]:
            campaigns = AD_CAMPAIGNS["paid_search"] if platform == "google_ads" else AD_CAMPAIGNS["paid_social"]

            for camp in campaigns:
                spend = round(daily_budget * random.uniform(0.12, 0.25) * season, 2)

                # Metrics vary by campaign type
                if platform == "google_ads":
                    cpc        = round(random.uniform(0.55, 2.20), 2)
                    clicks     = int(spend / cpc)
                    impressions = int(clicks / random.uniform(0.03, 0.06))
                    ctr        = round(clicks / impressions, 4) if impressions > 0 else 0
                    conversions = int(clicks * random.uniform(0.03, 0.08))
                    conv_value  = round(conversions * random.uniform(28, 55), 2)
                else:
                    cpm        = round(random.uniform(8, 22), 2)
                    impressions = int((spend / cpm) * 1000)
                    ctr        = round(random.uniform(0.008, 0.025), 4)
                    clicks     = int(impressions * ctr)
                    conversions = int(clicks * random.uniform(0.015, 0.04))
                    conv_value  = round(conversions * random.uniform(28, 55), 2)

                roas = round(conv_value / spend, 2) if spend > 0 else 0

                rows.append({
                    "spend_id":       f"AD{len(rows)+1:08d}",
                    "date":           date.strftime("%Y-%m-%d"),
                    "platform":       platform,
                    "campaign_id":    camp["id"],
                    "campaign_name":  camp["name"],
                    "impressions":    impressions,
                    "clicks":         clicks,
                    "ctr":            ctr,
                    "spend":          spend,
                    "conversions":    conversions,
                    "conversion_value": conv_value,
                    "roas":           roas,
                })

    return pd.DataFrame(rows)


# ── Table 5: Email Events ─────────────────────────────────────────────────────

def generate_email_events(customers_df):
    print("  Generating email events...")
    rows        = []
    event_id    = 1
    opted_in    = customers_df[customers_df["email_opt_in"] == True]

    for _, cust in opted_in.iterrows():
        signup = datetime.strptime(cust["signup_date"], "%Y-%m-%d")

        for flow in EMAIL_FLOWS:
            # Not every customer enters every flow
            if flow == "welcome_series":
                entry_prob = 1.0
            elif flow == "post_purchase":
                entry_prob = 0.85
            elif flow == "abandoned_cart":
                entry_prob = 0.30
            elif flow == "win_back":
                entry_prob = 0.20
            elif flow == "promotional_blast":
                entry_prob = 0.90
            elif flow == "subscription_renewal":
                entry_prob = 0.18 if cust["is_subscriber"] else 0.0

            if random.random() > entry_prob:
                continue

            # Number of emails in this flow
            n_emails = {
                "welcome_series":      random.choice([3, 4, 5]),
                "post_purchase":       random.choice([2, 3]),
                "abandoned_cart":      random.choice([2, 3]),
                "win_back":            random.choice([3, 4]),
                "promotional_blast":   random.randint(4, 12),
                "subscription_renewal": random.choice([2, 3]),
            }[flow]

            open_rate_base  = random.uniform(0.18, 0.42)
            click_rate_base = random.uniform(0.03, 0.12)

            send_date = signup + timedelta(days=random.randint(0, 3))

            for email_num in range(1, n_emails + 1):
                if send_date > END_DATE:
                    break

                sent = True
                # Open rate decays with email fatigue
                fatigue = 1 - (email_num - 1) * 0.06
                opened  = sent and random.random() < (open_rate_base * fatigue)
                clicked = opened and random.random() < click_rate_base
                converted = clicked and random.random() < 0.12
                unsubscribed = (not converted) and random.random() < 0.008

                rows.append({
                    "event_id":      f"E{event_id:09d}",
                    "customer_id":   cust["customer_id"],
                    "email":         cust["email"],
                    "flow":          flow,
                    "email_number":  email_num,
                    "send_date":     send_date.strftime("%Y-%m-%d"),
                    "sent":          True,
                    "opened":        opened,
                    "clicked":       clicked,
                    "converted":     converted,
                    "unsubscribed":  unsubscribed,
                    "subject_line":  f"{flow.replace('_',' ').title()} #{email_num}",
                })
                event_id += 1

                if unsubscribed:
                    break

                send_date += timedelta(days=random.choice([2, 3, 5, 7]))

    return pd.DataFrame(rows)


# ── Run All ───────────────────────────────────────────────────────────────────

def main():
    print("\n🟤 NovaBrew Coffee — Data Generation")
    print("=" * 45)

    print("\n[1/5] Customers")
    customers_df = generate_customers(n=8000)
    customers_df.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
    print(f"      ✓ {len(customers_df):,} customers saved")

    print("\n[2/5] Orders & Order Items")
    orders_df, order_items_df = generate_orders(customers_df)
    orders_df.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False)
    order_items_df.to_csv(f"{OUTPUT_DIR}/order_items.csv", index=False)
    print(f"      ✓ {len(orders_df):,} orders saved")
    print(f"      ✓ {len(order_items_df):,} order items saved")

    print("\n[3/5] Sessions")
    sessions_df = generate_sessions(orders_df, n_sessions=180000)
    sessions_df.to_csv(f"{OUTPUT_DIR}/sessions.csv", index=False)
    print(f"      ✓ {len(sessions_df):,} sessions saved")

    print("\n[4/5] Ad Spend")
    ad_spend_df = generate_ad_spend()
    ad_spend_df.to_csv(f"{OUTPUT_DIR}/ad_spend.csv", index=False)
    print(f"      ✓ {len(ad_spend_df):,} ad spend rows saved")

    print("\n[5/5] Email Events")
    email_df = generate_email_events(customers_df)
    email_df.to_csv(f"{OUTPUT_DIR}/email_events.csv", index=False)
    print(f"      ✓ {len(email_df):,} email events saved")

    # ── Summary Stats ──────────────────────────────────────────────────────
    print("\n" + "=" * 45)
    print("📊 Dataset Summary")
    print("=" * 45)
    total_revenue = orders_df[orders_df["status"] == "delivered"]["order_total"].sum()
    total_spend   = ad_spend_df["spend"].sum()
    blended_roas  = total_revenue / total_spend if total_spend > 0 else 0
    cvr           = orders_df["order_id"].nunique() / len(sessions_df) * 100
    aov           = orders_df[orders_df["status"] == "delivered"]["order_total"].mean()

    print(f"  Period:         {START_DATE.strftime('%b %Y')} – {END_DATE.strftime('%b %Y')}")
    print(f"  Total Revenue:  ${total_revenue:>12,.2f}")
    print(f"  Total Ad Spend: ${total_spend:>12,.2f}")
    print(f"  Blended ROAS:   {blended_roas:>12.2f}x")
    print(f"  Avg Order Value:${aov:>12.2f}")
    print(f"  Conv. Rate:     {cvr:>11.2f}%")
    print(f"\n  Output files in: {OUTPUT_DIR}/")
    print("=" * 45)

    # ── BigQuery Schema Export ─────────────────────────────────────────────
    schemas = {
        "customers": [
            {"name": "customer_id",           "type": "STRING",  "mode": "REQUIRED"},
            {"name": "first_name",             "type": "STRING",  "mode": "NULLABLE"},
            {"name": "last_name",              "type": "STRING",  "mode": "NULLABLE"},
            {"name": "email",                  "type": "STRING",  "mode": "NULLABLE"},
            {"name": "state",                  "type": "STRING",  "mode": "NULLABLE"},
            {"name": "acquisition_channel",    "type": "STRING",  "mode": "NULLABLE"},
            {"name": "signup_date",            "type": "DATE",    "mode": "NULLABLE"},
            {"name": "is_subscriber",          "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "email_opt_in",           "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "date_of_birth",          "type": "DATE",    "mode": "NULLABLE"},
        ],
        "orders": [
            {"name": "order_id",       "type": "STRING",  "mode": "REQUIRED"},
            {"name": "customer_id",    "type": "STRING",  "mode": "NULLABLE"},
            {"name": "order_date",     "type": "DATE",    "mode": "NULLABLE"},
            {"name": "shipped_date",   "type": "DATE",    "mode": "NULLABLE"},
            {"name": "delivered_date", "type": "DATE",    "mode": "NULLABLE"},
            {"name": "channel",        "type": "STRING",  "mode": "NULLABLE"},
            {"name": "promo_code",     "type": "STRING",  "mode": "NULLABLE"},
            {"name": "discount_pct",   "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "subtotal",       "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "shipping_cost",  "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "tax",            "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "order_total",    "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "status",         "type": "STRING",  "mode": "NULLABLE"},
            {"name": "is_first_order", "type": "BOOLEAN", "mode": "NULLABLE"},
        ],
        "order_items": [
            {"name": "item_id",      "type": "STRING",  "mode": "REQUIRED"},
            {"name": "order_id",     "type": "STRING",  "mode": "NULLABLE"},
            {"name": "customer_id",  "type": "STRING",  "mode": "NULLABLE"},
            {"name": "product_id",   "type": "STRING",  "mode": "NULLABLE"},
            {"name": "product_name", "type": "STRING",  "mode": "NULLABLE"},
            {"name": "category",     "type": "STRING",  "mode": "NULLABLE"},
            {"name": "quantity",     "type": "INT64",   "mode": "NULLABLE"},
            {"name": "unit_price",   "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "discount_pct", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "line_total",   "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "cogs",         "type": "FLOAT64", "mode": "NULLABLE"},
        ],
        "sessions": [
            {"name": "session_id",       "type": "STRING",  "mode": "REQUIRED"},
            {"name": "session_date",     "type": "DATE",    "mode": "NULLABLE"},
            {"name": "channel",          "type": "STRING",  "mode": "NULLABLE"},
            {"name": "device",           "type": "STRING",  "mode": "NULLABLE"},
            {"name": "landing_page",     "type": "STRING",  "mode": "NULLABLE"},
            {"name": "pages_viewed",     "type": "INT64",   "mode": "NULLABLE"},
            {"name": "duration_seconds", "type": "INT64",   "mode": "NULLABLE"},
            {"name": "bounced",          "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "utm_source",       "type": "STRING",  "mode": "NULLABLE"},
            {"name": "utm_medium",       "type": "STRING",  "mode": "NULLABLE"},
            {"name": "utm_campaign",     "type": "STRING",  "mode": "NULLABLE"},
            {"name": "converted",        "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "order_id",         "type": "STRING",  "mode": "NULLABLE"},
        ],
        "ad_spend": [
            {"name": "spend_id",          "type": "STRING",  "mode": "REQUIRED"},
            {"name": "date",              "type": "DATE",    "mode": "NULLABLE"},
            {"name": "platform",          "type": "STRING",  "mode": "NULLABLE"},
            {"name": "campaign_id",       "type": "STRING",  "mode": "NULLABLE"},
            {"name": "campaign_name",     "type": "STRING",  "mode": "NULLABLE"},
            {"name": "impressions",       "type": "INT64",   "mode": "NULLABLE"},
            {"name": "clicks",            "type": "INT64",   "mode": "NULLABLE"},
            {"name": "ctr",               "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "spend",             "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "conversions",       "type": "INT64",   "mode": "NULLABLE"},
            {"name": "conversion_value",  "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "roas",              "type": "FLOAT64", "mode": "NULLABLE"},
        ],
        "email_events": [
            {"name": "event_id",     "type": "STRING",  "mode": "REQUIRED"},
            {"name": "customer_id",  "type": "STRING",  "mode": "NULLABLE"},
            {"name": "email",        "type": "STRING",  "mode": "NULLABLE"},
            {"name": "flow",         "type": "STRING",  "mode": "NULLABLE"},
            {"name": "email_number", "type": "INT64",   "mode": "NULLABLE"},
            {"name": "send_date",    "type": "DATE",    "mode": "NULLABLE"},
            {"name": "sent",         "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "opened",       "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "clicked",      "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "converted",    "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "unsubscribed", "type": "BOOLEAN", "mode": "NULLABLE"},
            {"name": "subject_line", "type": "STRING",  "mode": "NULLABLE"},
        ],
    }

    with open(f"{OUTPUT_DIR}/bigquery_schemas.json", "w") as f:
        json.dump(schemas, f, indent=2)
    print(f"\n  ✓ BigQuery schemas saved to {OUTPUT_DIR}/bigquery_schemas.json")
    print("\n✅ Data generation complete!\n")


if __name__ == "__main__":
    main()
