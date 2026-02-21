Telegram.WebApp.ready();
Telegram.WebApp.onEvent("themeChanged", function () {
  document.documentElement.className = Telegram.WebApp.colorScheme;
});

let params = new URLSearchParams(Telegram.WebApp.initData);
let user = JSON.parse(decodeURIComponent(params.get("user")));

let hash = params.get("hash");
params.delete("hash");
let checkDataString = Array.from(params.entries())
  .sort(([a], [b]) => a.localeCompare(b))
  .map(([key, value]) => `${key}=${value}`)
  .join("\n");

let url = new URL("https://functions.yandexcloud.net/d4ee4tfflc942eo83k74");

// user = { id: 164671585 };
// hash = "";
// checkDataString = "";

url.searchParams.set("user_id", user["id"]);
url.searchParams.set("hash", hash);
url.searchParams.set("checkDataString", checkDataString);

const forms = document.getElementById("forms");

const FORM = document.getElementById("form");
const SECTION = document.getElementById("section");

let data = {};

function addOptions(form, chat = null) {
  const options = form.querySelector('select[name="chat"]');
  data.chats.forEach((c) => {
    const option = document.createElement("option");
    option.value = c["group_id"] + " " + c["thread_id"];
    option.textContent = c["thread"] || c["group"];

    if (option.value === chat) {
      option.selected = true;
    }
    options.appendChild(option);
  });
}

(async function () {
  const res = await fetch(url);
  if (!res.ok) {
    return;
  }
  data = await res.json();

  data.forms.forEach((f) => {
    const form = FORM.content.cloneNode(true).firstElementChild;

    addOptions(form, f.chat);

    form.querySelector('input[name="day"]').value = f.day;
    form.querySelector('input[name="month"]').value = f.month;
    form.querySelector('input[name="year"]').value = f.year;
    form.querySelector('input[name="when"]').value = f.when;
    form.querySelector('input[name="what"]').value = f.what;
    form.querySelector('input[name="id"]').value = f.id;
    form.querySelector('input[name="time_zone"]').value = f.time_zone;
    form.querySelector('input[name="stop"]').checked = f.stop;

    form.lastElementChild.lastElementChild.onclick = delForm;
    forms.appendChild(form);
  });

  const form = FORM.content.cloneNode(true).firstElementChild;
  addOptions(form);

  form.onclick = addForm;
  forms.appendChild(form);
})();

function addForm(e) {
  e.preventDefault();

  const f = e.target.closest("form");
  f.onclick = null;
  f.lastElementChild.lastElementChild.onclick = delForm;

  const form = FORM.content.cloneNode(true).firstElementChild;
  addOptions(form);

  form.onclick = addForm;
  forms.appendChild(form);
}

function delForm(e) {
  e.preventDefault();
  const form = e.target.parentElement.parentElement;
  form.remove();
}

Telegram.WebApp.MainButton.show();
Telegram.WebApp.MainButton.setText("сохранить все напоминания");

Telegram.WebApp.MainButton.onClick(async () => {
  const data = [...forms.querySelectorAll("form")]
    .map((form) => {
      const formData = new FormData(form);
      const entries = Array.from(formData.entries());

      const result = Object.fromEntries(entries);
      return result;
    })
    .filter((f) => f.when && f.what);

  url.searchParams.set("forms", JSON.stringify(data));
  await fetch(url);

  Telegram.WebApp.close();
});
