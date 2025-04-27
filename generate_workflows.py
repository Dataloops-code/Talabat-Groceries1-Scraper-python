import os

# List of areas with their names and URLs
areas = [
    {"name": "الظهر", "url": "https://www.talabat.com/kuwait/groceries/59/dhaher"},
    {"name": "الرقه", "url": "https://www.talabat.com/kuwait/groceries/37/riqqa"},
    {"name": "هدية", "url": "https://www.talabat.com/kuwait/groceries/30/hadiya"},
    {"name": "المنقف", "url": "https://www.talabat.com/kuwait/groceries/32/mangaf"},
    {"name": "أبو حليفة", "url": "https://www.talabat.com/kuwait/groceries/2/abu-halifa"},
    {"name": "الفنطاس", "url": "https://www.talabat.com/kuwait/groceries/38/fintas"},
    {"name": "العقيلة", "url": "https://www.talabat.com/kuwait/groceries/79/egaila"},
    {"name": "الصباحية", "url": "https://www.talabat.com/kuwait/groceries/31/sabahiya"},
    {"name": "الأحمدي", "url": "https://www.talabat.com/kuwait/groceries/3/al-ahmadi"},
    {"name": "الفحيحيل", "url": "https://www.talabat.com/kuwait/groceries/5/fahaheel"},
    {"name": "شرق الأحمدي", "url": "https://www.talabat.com/kuwait/groceries/3/al-ahmadi"},
    {"name": "ضاحية علي صباح السالم", "url": "https://www.talabat.com/kuwait/groceries/82/ali-sabah-al-salem-umm-al-hayman"},
    {"name": "ميناء عبد الله", "url": "https://www.talabat.com/kuwait/groceries/100/mina-abdullah"},
    {"name": "بنيدر", "url": "https://www.talabat.com/kuwait/groceries/6650/bnaider"},
    {"name": "الزور", "url": "https://www.talabat.com/kuwait/groceries/2053/zour"},
    {"name": "الجليعة", "url": "https://www.talabat.com/kuwait/groceries/6860/al-julaiaa"},
    {"name": "المهبولة", "url": "https://www.talabat.com/kuwait/groceries/24/mahboula"},
    {"name": "النويصيب", "url": "https://www.talabat.com/kuwait/groceries/2054/nuwaiseeb"},
    {"name": "الخيران", "url": "https://www.talabat.com/kuwait/groceries/2726/khairan"},
    {"name": "الوفرة", "url": "https://www.talabat.com/kuwait/groceries/2057/wafra-farms"},
    {"name": "ضاحية فهد الأحمد", "url": "https://www.talabat.com/kuwait/groceries/98/fahad-al-ahmed"},
    {"name": "ضاحية جابر العلي", "url": "https://www.talabat.com/kuwait/groceries/60/jaber-al-ali"},
    {"name": "مدينة صباح الأحمد السكنية", "url": "https://www.talabat.com/kuwait/groceries/6931/sabah-al-ahmad-2"},
    {"name": "مدينة صباح الأحمد البحرية", "url": "https://www.talabat.com/kuwait/groceries/2726/khairan"},
    {"name": "ميناء الأحمدي", "url": "https://www.talabat.com/kuwait/groceries/3/al-ahmadi"},
]
# Create workflows directory if it doesn't exist
os.makedirs(".github/workflows", exist_ok=True)

# Generate a workflow file for each area
for area in areas:
    area_name = area["name"]
    area_url = area["url"]
    workflow_content = template.format(area_name=area_name, area_url=area_url)
    workflow_filename = f".github/workflows/scrape_{area_name}.yml"
    with open(workflow_filename, "w", encoding="utf-8") as f:
        f.write(workflow_content)
    print(f"Generated {workflow_filename}")

print("All workflow files generated successfully!")
