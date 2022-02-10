const EMAIL_ALLOWLIST = ["fennel.ai", "trell.in"];

export const validateEmail = (username) => {
  const email = username.split("@");
  if (!email || email.length < 2) {
    throw new Error("Email is improperly formatted or missing.");
  }
  const domain = email[1];
  if (!EMAIL_ALLOWLIST.includes(domain)) {
    throw new Error("Email is not in allowed domains");
  }
};
