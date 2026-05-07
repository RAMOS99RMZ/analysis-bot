# 🎖️ Ramos Ai 360 ♾️🎖️
## دليل التطبيق الكامل من الصفر — للمبتدئين

> **اقرأ هذا الدليل كاملاً مرة واحدة قبل أي خطوة. كل شيء مشروح بالتفصيل.**

---

# الفصل الأول: ماذا بنينا؟

بوت تداول احترافي بلغة Python يعمل 24/7 على GitHub.
يحلل 7 عملات رقمية باستخدام **11 خبيراً** و**ذكاء اصطناعي** ويرسل إشارات فورية على Telegram.

## هيكل الملفات الكامل

```
ramos_ai_360/
│
├── main.py                        ← نقطة البداية الوحيدة
├── config.py                      ← كل الإعدادات والمعاملات
├── requirements.txt               ← قائمة المكتبات
├── .env.example                   ← نموذج الأسرار (انسخه لـ .env)
├── .gitignore                     ← يحمي ملفاتك السرية
│
├── engine/                        ← محرك التحليل
│   ├── data_fetcher.py            ← يجلب بيانات OKX (سعر، شموع، أوردر بوك)
│   ├── indicator_engine.py        ← يحسب RSI, MACD, ATR, Ichimoku... إلخ
│   ├── signal_generator.py        ← يجمع آراء الـ11 خبير ويبني الإشارة
│   └── risk_manager.py            ← يحسب حجم الصفقة، SL، TP
│
├── strategies/                    ← الـ 11 خبير
│   ├── expert_01_classic_ta.py    ← E1: RSI، MACD، BB، PSAR
│   ├── expert_02_harmonic.py      ← E2: Gartley، Bat، Butterfly، Crab
│   ├── expert_03_wyckoff.py       ← E3: Wyckoff (Accumulation/Distribution)
│   ├── expert_04_smc.py           ← E4: Smart Money (OB، FVG، BOS)
│   ├── expert_05_onchain.py       ← E5: Funding Rate، Order Book
│   ├── expert_06_sessions.py      ← E6: London، NY، Silver Bullet
│   ├── expert_07_fear_greed.py    ← E7: مؤشر الخوف والطمع
│   ├── expert_08_gann.py          ← E8: Gann + الدورة القمرية
│   ├── expert_09_obv.py           ← E9: OBV، CMF، Volume Profile
│   ├── expert_10_daily.py         ← E10: اتجاه يومي، CME Gap، POC
│   └── expert_11_usdt.py          ← E11: USDT Dominance
│
├── ai/
│   └── confirmation.py            ← تأكيد الإشارة بـ Grok AI + Gemini AI
│
├── database/
│   └── supabase_client.py         ← تسجيل الإشارات في Supabase
│
├── notifier/
│   └── telegram.py                ← إرسال الإشارات لـ Telegram
│
├── scheduler/
│   └── jobs.py                    ← جدولة المهام (5م، 15م، 2س، 4س)
│
├── backtesting/
│   └── backtest_engine.py         ← اختبار الاستراتيجيات تاريخياً
│
├── utils/
│   └── helpers.py                 ← أدوات مساعدة مشتركة
│
├── logs/                          ← سجلات البوت (تُنشأ تلقائياً)
│
└── .github/
    └── workflows/
        └── run_bot.yml            ← تشغيل 24/7 على GitHub Actions
```

---

# الفصل الثاني: التثبيت خطوة بخطوة

## الأدوات المطلوبة

| الأداة | الرابط | ملاحظة |
|--------|--------|---------|
| Python 3.11 | https://python.org/downloads | **مهم: 3.11 تحديداً** |
| Git | https://git-scm.com | لرفع الكود |
| VS Code | https://code.visualstudio.com | لتحرير الملفات |

---

## الخطوة 1: تثبيت Python

### على Windows:
1. افتح الرابط: https://python.org/downloads
2. حمّل Python 3.11.x
3. **مهم جداً**: ضع ✅ على "Add Python to PATH" قبل التثبيت
4. انقر Install Now

### للتأكد أنه تثبّت صح، افتح Command Prompt واكتب:
```
python --version
```
يجب أن يظهر: `Python 3.11.x`

---

## الخطوة 2: تحميل المشروع

افتح مجلداً فارغاً على سطح المكتب. ثم افتح Terminal داخله:

**على Windows:** انقر يمين داخل المجلد → "Open in Terminal"

```bash
# تحميل المشروع من GitHub (بعد أن ترفعه)
git clone https://github.com/YOUR_USERNAME/ramos_ai_360.git
cd ramos_ai_360

# إذا لديك الملفات محلياً، تجاهل السطرين أعلاه وفقط:
cd ramos_ai_360
```

---

## الخطوة 3: إنشاء بيئة Python معزولة

```bash
# إنشاء البيئة الافتراضية
python -m venv venv

# تفعيلها (Windows):
venv\Scripts\activate

# تفعيلها (Mac/Linux):
source venv/bin/activate
```

**علامة النجاح:** سترى `(venv)` في بداية سطر الأوامر.

---

## الخطوة 4: تثبيت المكتبات

```bash
pip install -r requirements.txt
```

⏳ **هذا يستغرق 3-8 دقائق.** انتظر حتى ينتهي.

---

## الخطوة 5: إعداد ملف الأسرار

```bash
# انسخ الملف النموذجي
# Windows:
copy .env.example .env

# Mac/Linux:
cp .env.example .env
```

ثم افتح ملف `.env` بـ VS Code وعدّله (الشرح في الفصل التالي).

---

# الفصل الثالث: الحصول على المفاتيح السرية

## 🔑 1. مفاتيح OKX

1. سجّل دخول لحساب OKX
2. اذهب لـ: **Profile → API Management**
3. انقر **"Create API Key"**
4. الاسم: `ramos_bot`
5. الصلاحيات: ✅ **Read** + ✅ **Trade** فقط (لا تفعّل Withdraw أبداً!)
6. أضف IP Whitelist إذا كنت تعرف IP خادمك
7. انسخ الثلاثة: **API Key** + **Secret Key** + **Passphrase**

```env
OKX_KEY=abc123def456...
OKX_SECRET=xyz789ghi012...
OKX_PASS=كلمة_المرور_التي_اخترتها
```

---

## 📨 2. بوت Telegram

### إنشاء البوت:
1. افتح Telegram وابحث عن `@BotFather`
2. أرسل: `/newbot`
3. اختر اسماً للبوت (مثل: `RamosAI360Bot`)
4. احفظ الـ **Token** الذي يعطيك إياه

### معرفة CHAT_ID:
1. أرسل أي رسالة لبوتك الجديد
2. افتح هذا الرابط في المتصفح (عوّض TOKEN بالرمز):
   `https://api.telegram.org/botTOKEN/getUpdates`
3. ابحث عن `"chat":{"id":` — الرقم بعدها هو CHAT_ID

```env
BOT_TOKEN=1234567890:AAFxxxYYYzzz...
CHAT_ID=987654321
```

---

## 🤖 3. Grok AI (xAI)

1. افتح: https://console.x.ai/
2. سجّل دخول بحساب X (تويتر)
3. اذهب لـ API Keys → Create Key
4. احفظ المفتاح

```env
GROQ_KEY=gsk_xxxxxxxxxxxx...
```

---

## 🧠 4. Gemini AI (Google)

1. افتح: https://aistudio.google.com/app/apikey
2. انقر "Create API Key"
3. احفظ المفتاح

```env
GEMINI_KEY=AIzaSyxxxxxxxxxx...
```

---

## 🗄️ 5. Supabase (قاعدة البيانات)

1. افتح: https://supabase.com وسجّل حساباً مجانياً
2. انقر **"New Project"**
3. الاسم: `ramos-ai-360`
4. اختر كلمة مرور قوية واحفظها
5. انتظر حتى ينتهي إنشاء المشروع (~60 ثانية)
6. اذهب لـ: **Settings → API**
7. احفظ:
   - **Project URL** → هذا هو `SUPABASE_URL`
   - **anon public key** → هذا هو `SUPABASE_KEY`

```env
SUPABASE_URL=https://abcdefghij.supabase.co
SUPABASE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

### إنشاء جداول Supabase:
1. في لوحة Supabase، اذهب لـ **SQL Editor**
2. انسخ هذا الكود وانقر **Run**:

```sql
CREATE TABLE IF NOT EXISTS signals (
    id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW(),
    symbol TEXT, direction TEXT, trade_type TEXT, score FLOAT,
    expert_votes INT, mtf_score FLOAT, entry_price FLOAT,
    sl_price FLOAT, tp1_price FLOAT, tp2_price FLOAT, tp3_price FLOAT,
    size_usdt FLOAT, regime TEXT, timeframe TEXT,
    experts_fired TEXT[], ai_confirmation TEXT, run_id TEXT
);
CREATE TABLE IF NOT EXISTS trades (
    id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW(),
    symbol TEXT, direction TEXT, trade_type TEXT, status TEXT,
    entry_price FLOAT, exit_price FLOAT, pnl_pct FLOAT,
    size_usdt FLOAT, run_id TEXT
);
CREATE TABLE IF NOT EXISTS performance (
    id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW(),
    symbol TEXT, wins INT, losses INT, total INT,
    win_rate FLOAT, total_pnl FLOAT, max_dd FLOAT
);
CREATE TABLE IF NOT EXISTS regime_log (
    id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW(),
    symbol TEXT, regime TEXT, confidence FLOAT
);
CREATE TABLE IF NOT EXISTS bot_heartbeat (
    id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW(),
    version TEXT, active_assets TEXT[], status TEXT
);
```

---

# الفصل الرابع: تشغيل البوت محلياً (للاختبار)

```bash
# تأكد أن البيئة مفعّلة
# Windows:
venv\Scripts\activate

# شغّل البوت
python main.py
```

### علامات النجاح التي ستراها:
```
✅ All mandatory secrets loaded successfully.
✅ Supabase connected successfully.
✅ 🎖️ Ramos Ai 360 ♾️🎖️ is ONLINE  ← رسالة Telegram
⏱  APScheduler running — all jobs registered
   🔍 monitor_positions  → every 5m
   ⚡ run_scalp          → every 15m
   🌊 run_swing          → every 2h
```

### إيقاف البوت:
اضغط `Ctrl + C`

---

# الفصل الخامس: النشر 24/7 على GitHub

## الخطوة 1: إنشاء مستودع GitHub

1. افتح https://github.com وسجّل دخول
2. انقر **"New repository"**
3. الاسم: `ramos_ai_360`
4. الخصوصية: **Private** (مهم للأمان!)
5. انقر **Create repository**

---

## الخطوة 2: رفع الكود

```bash
git init
git add .
git commit -m "🚀 Ramos Ai 360 — full Python migration"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/ramos_ai_360.git
git push -u origin main
```

---

## الخطوة 3: إضافة الأسرار لـ GitHub

1. في صفحة مستودعك على GitHub
2. اذهب لـ: **Settings → Secrets and variables → Actions**
3. انقر **"New repository secret"**
4. أضف كل هذه الأسرار واحداً واحداً:

| الاسم | القيمة |
|-------|--------|
| `OKX_KEY` | مفتاح OKX |
| `OKX_SECRET` | سر OKX |
| `OKX_PASS` | باسوورد OKX |
| `BOT_TOKEN` | رمز Telegram |
| `CHAT_ID` | معرف المحادثة |
| `GROQ_KEY` | مفتاح Grok |
| `GEMINI_KEY` | مفتاح Gemini |
| `SUPABASE_URL` | رابط Supabase |
| `SUPABASE_KEY` | مفتاح Supabase |

---

## الخطوة 4: تفعيل GitHub Actions

1. اذهب لتبويب **"Actions"** في مستودعك
2. انقر **"Enable Actions"** إذا ظهر
3. ملف `.github/workflows/run_bot.yml` يشغّل البوت تلقائياً كل 10 دقائق

---

## كيف يعمل النظام؟

```
كل 10 دقائق
     ↓
GitHub يشغّل: python main.py
     ↓
APScheduler يدير المهام داخلياً:
┌─────────────────────────────────────┐
│  كل 5  دق  → فحص المراكز المفتوحة │
│  كل 15 دق  → تحليل Scalp           │
│  كل 2  ساعة → تحليل Swing          │
│  كل 4  ساعة → تحليل Super Swing    │
│  يومياً     → تقرير الأداء         │
└─────────────────────────────────────┘
     ↓
الإشارات → Telegram فوراً
البيانات → Supabase للحفظ
```

---

# الفصل السادس: كيف تقرأ إشارة البوت؟

عندما يجد البوت فرصة، يرسل رسالة مثل:

```
🟢⚡ BTC — LONG SCALP
━━━━━━━━━━━━━━━━━━━━━━━━
📊 Score: 0.73 | Votes: 8
🌍 Regime: TRENDING
━━━━━━━━━━━━━━━━━━━━━━━━
🎯 Entry:  67,234.5000
🛡 SL:     66,800.0000
✅ TP1:   67,500.0000
✅ TP2:   67,900.0000
✅ TP3:   68,500.0000
━━━━━━━━━━━━━━━━━━━━━━━━
💰 Size: $450.00
🤖 Experts: ClassicTA, SMC, OBV, Sessions
🧠 AI: YES
━━━━━━━━━━━━━━━━━━━━━━━━
🎖️ Ramos Ai 360
```

### شرح كل حقل:

| الحقل | المعنى |
|-------|--------|
| 🟢 = LONG / 🔴 = SHORT | اتجاه الصفقة |
| ⚡ = Scalp / 🌊 = Swing | نوع الصفقة |
| Score | نقاط الإشارة من 0 إلى 1 (0.70+ ممتاز) |
| Votes | عدد الخبراء الموافقين من 11 |
| Entry | سعر الدخول المقترح |
| SL | وقف الخسارة — لا تتجاوزه |
| TP1/2/3 | أهداف الربح التدريجية |
| Size | حجم الصفقة بالدولار |
| AI: YES | Grok + Gemini وافقا على الإشارة |

---

# الفصل السابع: الأخطاء الشائعة وحلولها

## ❌ `ModuleNotFoundError`
```bash
# الحل: تأكد أن البيئة الافتراضية مفعّلة
# Windows:
venv\Scripts\activate
pip install -r requirements.txt
```

## ❌ `RuntimeError: MISSING MANDATORY SECRETS`
```bash
# الحل: تحقق من ملف .env
# Windows: افتحه بـ Notepad
notepad .env
```

## ❌ `Supabase connection failed`
- تأكد أن `SUPABASE_URL` يبدأ بـ `https://`
- تأكد أن مشروع Supabase نشط وليس في وضع Sleep

## ❌ `ccxt.errors.AuthenticationError`
- تحقق من `OKX_KEY` و`OKX_SECRET` و`OKX_PASS`
- تأكد أن مفتاح OKX له صلاحية Trade

## ❌ البوت لا يرسل لـ Telegram
```bash
# اختبر BOT_TOKEN يدوياً:
# افتح المتصفح وأدخل (عوّض TOKEN):
# https://api.telegram.org/botTOKEN/getMe
# يجب أن يظهر اسم البوت
```

## ❌ `vectorbt` لا يثبّت
```bash
pip install vectorbt --no-deps
pip install -r requirements.txt --ignore-installed vectorbt
```

---

# الفصل الثامن: الأمان والحماية

### ✅ قواعد ذهبية لا تخالفها:

1. **لا تشارك ملف `.env` مع أحد إطلاقاً**
2. **مفاتيح OKX: Trade فقط — لا Withdraw أبداً**
3. **المستودع Private دائماً**
4. **غيّر مفاتيح OKX فوراً إذا اشتبهت بتسريب**
5. **لا تكتب أي مفتاح داخل الكود**

---

# الفصل التاسع: المراقبة والصيانة

### مراقبة البوت يومياً:
- **Telegram**: كل الإشارات والأخطاء تأتي هنا فوراً
- **GitHub Actions**: راقب تبويب Actions للتأكد من التشغيل
- **Supabase**: راقب جدول `bot_heartbeat` (نبضة كل 6 ساعات)

### جدول المراجعة الأسبوعية:
- الاثنين: راجع تقرير الأسبوع (يرسله البوت تلقائياً 08:00 UTC)
- راجع win rate في Supabase → جدول `performance`
- إذا Win Rate < 50%، راجع إعدادات `MIN_SCORE_10` في `config.py`

---

# الفصل العاشر: التخصيص والتعديل

### تعديل العملات المُراقبة:
في `config.py`، ابحث عن `ASSETS`:
```python
ASSETS: List[str] = field(default_factory=lambda: [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    # أضف هنا عملات أخرى بنفس الصيغة
])
```

### تعديل حجم المخاطرة:
```python
RISK_PERCENT: float = 1.0   # 1% من الرصيد لكل صفقة
```

### تشديد أو تخفيف شروط الإشارة:
```python
MIN_SCORE_10:     float = 2.5  # ارفع للتشديد، اخفض للتساهل
MIN_EXPERT_VOTES: int   = 3    # أدنى عدد خبراء موافقين
```

---

# الملخص السريع — 10 خطوات فقط

```
1. ثبّت Python 3.11
2. فعّل البيئة الافتراضية (venv)
3. نفّذ: pip install -r requirements.txt
4. احصل على مفاتيح: OKX + Telegram + Grok + Gemini
5. أنشئ مشروع Supabase وأنشئ الجداول (SQL)
6. انسخ .env.example إلى .env وأملأه
7. اختبر محلياً: python main.py
8. أنشئ مستودع GitHub Private
9. أضف الأسرار في Settings → Secrets
10. ارفع الكود وراقب تبويب Actions
```

---

*🎖️ Ramos Ai 360 ♾️🎖️ — مبني بالحب والدقة*
*Python Migration — All sessions complete*
