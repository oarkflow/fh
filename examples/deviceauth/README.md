# Device authentication demo

This example includes a browser UI and terminal client for passwordless
registration requests, admin approval, device-key login, recovery, and
protected API access.

`FH_DEVICEAUTH_ADMIN_TOKEN` is the administrator's shared secret. It is not a
user token or registration invite. The server uses it only to authorize the
admin invite-creation endpoint. Start the loopback-only server with that
admin secret configured:

```sh
FH_DEVICEAUTH_ADMIN_TOKEN='ADMIN_SECRET' go run ./examples/deviceauth server
```

Open [http://127.0.0.1:8093](http://127.0.0.1:8093) in a browser. The loopback
host is a secure context for browser WebCrypto and IndexedDB.

Users enter an account ID and email, then enter the six-digit code sent to that
address. The server checks account and email syntax, blocks duplicate and
high-rate requests, verifies email ownership, and automatically approves an
eligible signup. The browser never asks users for an admin secret or invite
code. Verification codes expire in 10 minutes and allow five attempts. For a
real email delivery setup, configure `FH_DEVICEAUTH_SMTP_HOST`,
`FH_DEVICEAUTH_SMTP_PORT`, `FH_DEVICEAUTH_SMTP_USER`,
`FH_DEVICEAUTH_SMTP_PASSWORD`, and `FH_DEVICEAUTH_EMAIL_FROM`. With no SMTP host
configured, this loopback-only demo prints codes to the server console for
local development. Registration requests are held in memory and disappear
when the server restarts. The browser generates an Ed25519 key pair; its
private key is non-extractable and stored in IndexedDB. Login is one click on a
browser that already has that device key.

For the terminal client, an administrator can create a one-time invite:

```sh
FH_DEVICEAUTH_ADMIN_TOKEN='ADMIN_SECRET' go run ./examples/deviceauth admin -user alice
go run ./examples/deviceauth client -action register -user alice -invite-code 'INVITE_FROM_ADMIN'
```

Registration displays a one-time recovery code. Save it before closing the
dialog. The browser then signs in and displays the protected `/api/me` response.
Use **Check protected session** to make another signed request. The browser
keeps its non-extractable private key in IndexedDB; signing out clears the
session token but preserves that device key for later logins.

The terminal client stores the device private key and device ID in
`deviceauth-client.json` with mode `0600`. Log in and access the protected API:

```sh
go run ./examples/deviceauth client -action login -user alice
```

Change the user's registered device while authenticated:

```sh
go run ./examples/deviceauth client -action change -user alice
```

If the device state is lost, recover the account using the recovery code from
registration. Recovery enrolls a new key, removes the old device, rotates the
recovery code, and replaces the local state file:

```sh
go run ./examples/deviceauth client -action recover -user alice -recovery-code 'RECOVERY_CODE'
```

The browser's **Replace this device key** action requires an authenticated
session and revokes the old key after registering the replacement. The
**Account recovery** form uses the saved recovery code to enroll a key in this
browser, revokes the previous device, and returns a rotated recovery code. The
terminal client also supports `-action demo` for registration followed by
login; it requires `-invite-code` on first use. Use `-state /path/to/device.json`
to keep terminal client accounts separate.

The admin token authorizes manual invite creation for the terminal flow; it is
not a user credential. Browser signup auto-approval creates a high-entropy,
user-bound invite after email verification. Recovery uses a high-entropy one-time recovery
code rather than email reset; device change requires a currently authenticated
device. This example intentionally has no password credential.

The server binds to loopback and keeps invites, accounts, devices, challenges,
and replay state in memory with an ephemeral signing key. Restarting clears
everything. The browser uses plain HTTP only on loopback; deploy only behind
HTTPS. This is a complete runnable flow example, not a production identity
service: a real deployment needs durable shared stores, rate limits, invite and
recovery delivery, abuse controls, and operational key management. Recovery is
code-based, not email-based, and this passwordless design does not have a
password reset flow.
