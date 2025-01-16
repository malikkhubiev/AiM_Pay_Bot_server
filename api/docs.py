from loader import *
from fastapi import Response

@app.get("/offer")
async def get_offer():
    pdf_path = "../documents/offer.pdf"
    with open(pdf_path, "rb") as file:
        pdf_data = file.read()
    return Response(content=pdf_data, media_type="application/pdf", headers={
        "Content-Disposition": "inline; filename=offer.pdf"
    })
    
@app.get("/privacy")
async def get_privacy():
    pdf_path = "../documents/privacy.pdf"  # Путь к вашему PDF-файлу
    with open(pdf_path, "rb") as file:
        pdf_data = file.read()
    return Response(content=pdf_data, media_type="application/pdf", headers={
        "Content-Disposition": "inline; filename=privacy.pdf"
    })
