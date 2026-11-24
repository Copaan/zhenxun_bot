from zhenxun.configs.path_config import DATA_PATH, IMAGE_PATH, THEMES_PATH

SIGN_TODAY_CARD_PATH = IMAGE_PATH / "sign" / "today_card"
SIGN_HOLIDAY_PATH = DATA_PATH / "sign_in" / "holiday"
"""法定节假日安排的按年缓存目录"""
SIGN_HOLIDAY_URLS = (
    "https://cdn.jsdelivr.net/gh/NateScarlet/holiday-cn@master/{year}.json",
    "https://raw.githubusercontent.com/NateScarlet/holiday-cn/master/{year}.json",
)
"""holiday-cn 法定节假日数据地址"""
SIGN_FIGURE_PATH = THEMES_PATH / "default" / "assets" / "ui" / "mahiro"
"""默认主题中签到卡片真寻立绘所在目录"""

SIGN_GREETINGS = (
    (
        0,
        5,
        (
            "night/149001105.webp",
            "night/123430532.webp",
            "night/131520918-0.webp",
            "night/127657930.webp",
            "night/145931793.webp",
        ),
        (
            "今天要早点休息哦~",
            "可不要熬夜到太晚呀",
            "请尽早休息吧！",
            "不要熬夜啦！",
            "这么晚还来签到，是不是又在熬夜？",
            "夜深了，签完就快去睡觉吧",
            "{name}已经困得睁不开眼了……",
        ),
    ),
    (
        5,
        10,
        (
            "day/126833712.webp",
            "day/128290410.webp",
            "day/114293683.webp",
            "day/147438979.webp",
            "day/143062804.webp",
        ),
        (
            "早上好，希望今天是美好的一天！",
            "醒了吗，今天也要元气满满哦！",
            "早上好呀，今天也要开心哦！",
            "早安，愿你拥有美好的一天！",
            "早起签到，好运加倍~",
            "吃早饭了吗？不吃早饭可不行哦",
        ),
    ),
    (
        10,
        14,
        (
            "day/133934441.webp",
            "day/139187245.webp",
            "day/127277055.webp",
            "day/140184320.webp",
            "day/138494183.webp",
        ),
        (
            "中午好，忙了一上午辛苦啦",
            "午饭吃了吗？记得按时吃饭哦",
            "吃饱了才有力气干活！",
            "午休一下，下午会更有精神哦",
            "{name}希望你开心！",
        ),
    ),
    (
        14,
        18,
        (
            "day/130444369.webp",
            "day/132127627.webp",
            "day/151224709.webp",
            "day/145120555.webp",
            "day/142961184.webp",
        ),
        (
            "下午也要加油哦！",
            "来杯下午茶怎么样？",
            "犯困的话就起来走走吧",
            "记得多喝水哦",
            "{name}希望你开心！",
        ),
    ),
    (
        18,
        23,
        (
            "day/131520918-1.webp",
            "day/144860034.webp",
            "day/150992028.webp",
            "night/123430532.webp",
            "night/127579503.webp",
        ),
        (
            "晚上好，今天过得怎么样？",
            "辛苦一天了，好好放松一下吧",
            "晚饭要好好吃哦",
            "到家了吗？路上注意安全",
            "{name}希望你开心！",
        ),
    ),
    (
        23,
        24,
        (
            "night/149001105.webp",
            "night/123430532.webp",
            "night/131520918-0.webp",
            "night/127657930.webp",
            "night/145931793.webp",
        ),
        (
            "今天要早点休息哦~",
            "快到明天啦，该准备睡觉了",
            "签完到就去睡吧，晚安~",
        ),
    ),
)
"""签到寄语，按小时段排列：起始小时、结束小时（不含）、真寻立绘、寄语；
立绘为主题 `ui/mahiro` 目录下的相对路径，寄语中的 {name} 替换为 Bot 昵称"""

SIGN_DOUBLE_GREETING = (
    (
        "day/114293683.webp",
        "day/126833712.webp",
        "day/143062804.webp",
    ),
    (
        "night/123430532.webp",
        "night/127579503.webp",
        "night/149001105.webp",
    ),
    (
        "今天运气超好，好感翻倍啦！",
        "双倍好感！今天一定是幸运日~",
        "哇，翻倍了！{name}也替你开心！",
    ),
)
"""好感双倍时白天、夜间的真寻立绘与寄语"""


lik2relation = {
    "0": "路人",
    "1": "陌生",
    "2": "初识",
    "3": "普通",
    "4": "熟悉",
    "5": "信赖",
    "6": "相知",
    "7": "厚谊",
    "8": "亲密",
}

level2attitude = {
    "0": "排斥",
    "1": "警惕",
    "2": "可以交流",
    "3": "一般",
    "4": "是个好人",
    "5": "好朋友",
    "6": "可以分享小秘密",
    "7": "喜欢",
    "8": "恋人",
}

lik2level = {
    400: "8",
    270: "7",
    200: "6",
    140: "5",
    90: "4",
    50: "3",
    25: "2",
    10: "1",
    0: "0",
}
