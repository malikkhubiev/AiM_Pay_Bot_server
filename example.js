const form = document.querySelector('form');
form.addEventListener('submit', async function(e) {
    e.preventDefault();

    const name = form.querySelector('[name="name"]').value;
    const email = form.querySelector('[name="email"]').value;
    const phone = form.querySelector('[name="phone"]').value;

    if (!name || !email || !phone) {
        alert('Пожалуйста, заполните все поля');
        return;
    }

    try {
        const res = await fetch('https://aim-pay-bot-server.onrender.com/send_demo_link', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ name, email, phone })
        });
        const data = await res.json();
        if (res.ok && data.status === 'success') {
            alert('Спасибо! Мы отправили ссылку на вашу почту: ' + email);
            closeModal && closeModal();
            form.reset();
        } else {
            alert(data.message || 'Ошибка отправки. Попробуйте позже.');
        }
    } catch (err) {
        alert('Сеть недоступна. Попробуйте позже.');
    }
});