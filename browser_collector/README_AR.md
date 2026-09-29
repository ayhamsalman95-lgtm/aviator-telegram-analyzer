# Browser-side Collector — المرحلة التجريبية

هذه المرحلة تنقل التقاط أحداث SmartFox المرئية للمتصفح من Playwright على الخادم إلى متصفح المستخدم الذي سجّل دخوله بالفعل إلى 1xBet.

## البنية

~~~text
Chrome المستخدم
  └─ 1xBet + جلسة تسجيل الدخول
       └─ SmartFox extensionResponse
            ↓
       Chrome extension
            ↓ HTTPS
       /browser/events على الخادم
            ↓
       BrowserIngestor
            ↓
       RoundTracker + SQLite + Telegram outbox
~~~

الإضافة لا تقرأ كلمة مرور 1xBet أو Cookies أو sessionStorage. الـhook يراقب فقط `extensionResponse` في صفحة Aviator ويخفي أسماء الحقول الحساسة المعروفة.

## تثبيت الإضافة

1. افتح `chrome://extensions`.
2. فعّل Developer mode.
3. اختر Load unpacked.
4. اختر مجلد `browser_collector/extension`.

ثم افتح Options للإضافة وضع عنوان `/browser/events` للخادم وقيمة بيئة `BROWSER_COLLECTOR_TOKEN`.

## الخادم

ثبّت متطلبات المشروع ثم شغّل:

~~~bash
export BROWSER_COLLECTOR_TOKEN='قيمة عشوائية قوية'
python3 -m browser_collector.server
~~~

على Railway يستمع الخادم إلى `$PORT`.

## Telegram Mini App

لا ينبغي وضع 1xBet داخل iframe في Mini App وتوقع أن تشارك الجلسة أو SmartFox مع التطبيق؛ المتصفح يعزل JavaScript وCookies بين النطاقات، كما أن الموقع قد يفرض سياسة منع التضمين.

لذلك هذه المرحلة تجعل Chrome المستخدم هو مصدر الجلسة والبيانات. يمكن بناء Mini App لاحقًا كواجهة تحكم وحالة، بينما يستمر الالتقاط داخل Chrome.

الهدف جمع وتحليل البيانات فقط، بدون تنفيذ رهانات آلية.