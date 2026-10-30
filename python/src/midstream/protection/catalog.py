"""The kinds of sensitive data the rules recognise, and the masks a protected value can take.

A catalog in code, like permissions and settings: a kind has an id, a label and a description for
the console, the mask it gets by default, the masks it allows, the name rules that recognise it
(`rules.py`), and what its partial mask keeps (`mask.py`). Adding a kind is adding an entry here
with its rules, and its hits and near-misses to `test_protection_rules.py`.

Field names are matched in a normalised form: lowercase letters and digits only, so
`phone_number`, `phoneNumber`, `Phone-Number` and `PHONE NUMBER` are all `phonenumber`.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

#: Masks. `hash`: a keyed hash, equal values stay equal. `partial`: a fragment behind `****`.
#: `redact`: nothing is stored.
MASK_HASH = "hash"
MASK_PARTIAL = "partial"
MASK_REDACT = "redact"
MASKS = (MASK_HASH, MASK_PARTIAL, MASK_REDACT)

#: How the console names each mask.
MASK_LABELS = {MASK_HASH: "Hashed", MASK_PARTIAL: "Partial", MASK_REDACT: "Withheld"}
MASK_DESCRIPTIONS = {
    MASK_HASH: "A keyed hash: equal values stay equal, so joins and counts work; the value cannot be read",
    MASK_PARTIAL: "A readable fragment behind ****, such as the last four digits; not joinable",
    MASK_REDACT: "Not stored at all",
}

#: What a partial mask keeps (`mask.py`).
PARTIAL_LAST_DIGITS = "last_digits"
PARTIAL_LAST_CHARACTERS = "last_characters"
PARTIAL_EMAIL_DOMAIN = "email_domain"
PARTIAL_FIRST_LETTER = "first_letter"
PARTIAL_IP_PREFIX = "ip_prefix"
PARTIAL_YEAR = "year"
PARTIAL_NOTHING = "nothing"
PARTIAL_RULES = (
    PARTIAL_LAST_DIGITS,
    PARTIAL_LAST_CHARACTERS,
    PARTIAL_EMAIL_DOMAIN,
    PARTIAL_FIRST_LETTER,
    PARTIAL_IP_PREFIX,
    PARTIAL_YEAR,
    PARTIAL_NOTHING,
)

#: Leaves of a container that take the container's kind: `phone: {"number": …, "type": …}` masks
#: `number` and leaves `type`. Only a direct child inherits, and only under a container whose own
#: name a rule matched.
GENERIC_LEAVES = frozenset(
    {
        "number", "value", "raw", "text", "formatted", "display", "original", "normalized", "normalised",
        "e164", "national", "international", "full", "address", "addr", "line", "line1", "line2",
        "line3", "street", "digits", "first", "last", "given", "family", "middle", "maiden",
    }
)


@dataclass(frozen=True, slots=True)
class Rule:
    """Recognises a kind from a field's normalised name.

    The name matches when it is one of `exact`, or ends with one of `suffixes`, and is not one of
    `excluding`. With `under`, it matches only under a parent whose normalised name is one of them.
    """

    exact: frozenset[str] = frozenset()
    suffixes: tuple[str, ...] = ()
    under: frozenset[str] = frozenset()
    excluding: frozenset[str] = frozenset()

    def matches(self, name: str) -> bool:
        if name in self.excluding:
            return False
        return name in self.exact or any(name.endswith(suffix) for suffix in self.suffixes)


@dataclass(frozen=True, slots=True)
class Kind:
    id: str
    label: str
    description: str
    default_mask: str
    rules: tuple[Rule, ...]
    partial: str
    #: What `partial` makes of a typical value, for the console: (value, partial).
    partial_example: tuple[str, str]
    masks: tuple[str, ...] = MASKS
    #: Every leaf under a container of this kind takes the kind, not only the generic ones: a
    #: secret object's every value is secret.
    inherit_all: bool = False
    #: Names the console shows as examples of what the kind recognises.
    examples: tuple[str, ...] = field(default=())

    def allows(self, mask: str) -> bool:
        return mask in self.masks

    def to_dict(self) -> dict:
        """The kind as `GET /system/settings` describes it to the console."""
        return {
            "id": self.id,
            "label": self.label,
            "description": self.description,
            "default_mask": self.default_mask,
            "masks": list(self.masks),
            "partial_example": {"value": self.partial_example[0], "partial": self.partial_example[1]},
            "examples": list(self.examples),
        }


def _set(items: Iterable[str]) -> frozenset[str]:
    return frozenset(items)


_PERSON_PARENTS = _set(
    [
        "customer", "user", "person", "contact", "employee", "member", "passenger", "patient", "driver",
        "rider", "guest", "owner", "cardholder", "accountholder", "recipient", "sender", "buyer", "seller",
        "payer", "payee", "applicant", "subscriber", "traveler", "traveller", "student", "guardian",
        "beneficiary", "insured", "policyholder",
    ]
)
_ADDRESS_PARENTS = _set(["address", "billingaddress", "shippingaddress", "homeaddress", "mailingaddress", "residence"])

KINDS: tuple[Kind, ...] = (
    Kind(
        id="email",
        label="Email addresses",
        description="Fields named like email, email_address, contact_email",
        default_mask=MASK_HASH,
        rules=(
            Rule(
                exact=_set(["email", "emailaddress", "emailaddr", "emailid"]),
                suffixes=("email", "emailaddress"),
            ),
        ),
        partial=PARTIAL_EMAIL_DOMAIN,
        partial_example=("jane.doe@example.com", "****@example.com"),
        examples=("email", "email_address", "billing_email"),
    ),
    Kind(
        id="phone",
        label="Phone numbers",
        description="Fields named like phone, phone_number, mobile, fax, msisdn",
        default_mask=MASK_HASH,
        rules=(
            Rule(
                exact=_set(
                    [
                        "phone", "phonenumber", "phoneno", "phonenum", "telephone", "telephonenumber", "tel",
                        "mobile", "mobilenumber", "mobileno", "cellphone", "cellnumber", "fax", "faxnumber",
                        "msisdn",
                    ]
                ),
                suffixes=("phone", "phonenumber", "mobile", "msisdn"),
                excluding=_set(
                    [
                        "microphone", "headphone", "earphone", "iphone", "smartphone", "megaphone", "saxophone",
                        "xylophone", "gramophone", "automobile", "immobile",
                    ]
                ),
            ),
        ),
        partial=PARTIAL_LAST_DIGITS,
        partial_example=("540-948-9848", "****9848"),
        examples=("phone", "phone_number", "mobile"),
    ),
    Kind(
        id="person_name",
        label="People's names",
        description="first_name, last_name, full_name, and name under a person such as customer.name",
        default_mask=MASK_HASH,
        rules=(
            Rule(
                exact=_set(
                    [
                        "firstname", "lastname", "surname", "givenname", "familyname", "middlename", "fullname",
                        "maidenname", "legalname",
                    ]
                ),
                suffixes=("firstname", "lastname", "fullname"),
            ),
            Rule(exact=_set(["name", "displayname"]), under=_PERSON_PARENTS),
        ),
        partial=PARTIAL_FIRST_LETTER,
        partial_example=("Johansson", "J****"),
        examples=("first_name", "last_name", "customer.name"),
    ),
    Kind(
        id="national_id",
        label="National identifiers",
        description="Social security, tax, passport, driver's license and national ID numbers",
        default_mask=MASK_HASH,
        rules=(
            Rule(
                exact=_set(
                    [
                        "ssn", "socialsecuritynumber", "socialsecurityno", "nationalid", "nationalidnumber",
                        "nationalinsurancenumber", "nino", "taxid", "taxidnumber", "taxpayerid", "tin", "itin",
                        "passport", "passportnumber", "passportno", "driverslicense", "driverslicence",
                        "driverlicense", "driverlicence", "drivinglicense", "drivinglicence", "dlnumber",
                        "idcardnumber", "identitynumber", "identificationnumber", "personalid", "personalnumber",
                        "personnummer", "bsn", "cpf", "curp", "dni", "nif", "pesel", "aadhaar", "aadhaarnumber",
                        "sin", "nric", "hkid",
                    ]
                ),
            ),
        ),
        partial=PARTIAL_LAST_CHARACTERS,
        partial_example=("123-45-6789", "****6789"),
        examples=("ssn", "passport_number", "tax_id"),
    ),
    Kind(
        id="card_number",
        label="Payment card numbers",
        description="Credit and debit card numbers (PAN)",
        default_mask=MASK_HASH,
        rules=(
            Rule(
                exact=_set(
                    [
                        "cardnumber", "cardno", "cardnum", "creditcard", "creditcardnumber", "creditcardno",
                        "debitcard", "debitcardnumber", "ccnumber", "ccnum", "pan", "cardpan",
                        "primaryaccountnumber",
                    ]
                ),
                suffixes=("cardnumber",),
            ),
        ),
        partial=PARTIAL_LAST_DIGITS,
        partial_example=("4242 4242 4242 4242", "****4242"),
        examples=("card_number", "credit_card", "pan"),
    ),
    Kind(
        id="bank_account",
        label="Bank account numbers",
        description="IBAN, account and routing numbers, sort codes",
        default_mask=MASK_HASH,
        rules=(
            Rule(
                exact=_set(
                    [
                        "iban", "accountnumber", "accountno", "acctnumber", "acctno", "bankaccount",
                        "bankaccountnumber", "routingnumber", "routingno", "aba", "sortcode", "bsb", "clabe",
                    ]
                ),
            ),
        ),
        partial=PARTIAL_LAST_CHARACTERS,
        partial_example=("DE89 3704 0044 0532 0130 00", "****3000"),
        examples=("iban", "account_number", "routing_number"),
    ),
    Kind(
        id="address",
        label="Street addresses",
        description="Street address lines; city, region, country and postal code are left readable",
        default_mask=MASK_HASH,
        rules=(
            Rule(
                exact=_set(
                    [
                        "address", "streetaddress", "street", "addressline", "addressline1", "addressline2",
                        "addressline3", "address1", "address2", "address3", "homeaddress", "billingaddress",
                        "shippingaddress", "mailingaddress", "postaladdress", "residentialaddress", "housenumber",
                    ]
                ),
            ),
            Rule(exact=_set(["line1", "line2", "line3"]), under=_ADDRESS_PARENTS),
        ),
        partial=PARTIAL_NOTHING,
        partial_example=("221B Baker Street", "****"),
        examples=("street_address", "billing_address", "address.line1"),
    ),
    Kind(
        id="ip_address",
        label="IP addresses",
        description="Client, remote and source IP addresses",
        default_mask=MASK_HASH,
        rules=(
            Rule(
                exact=_set(
                    [
                        "ip", "ipaddress", "ipaddr", "clientip", "remoteip", "sourceip", "srcip", "remoteaddr",
                        "remoteaddress", "userip", "ipv4", "ipv6", "xforwardedfor", "forwardedfor",
                    ]
                ),
                suffixes=("ipaddress", "ipaddr"),
            ),
        ),
        partial=PARTIAL_IP_PREFIX,
        partial_example=("203.0.113.42", "203.0.*.*"),
        examples=("ip", "client_ip", "remote_addr"),
    ),
    Kind(
        id="date_of_birth",
        label="Dates of birth",
        description="Fields named like dob, date_of_birth, birthdate",
        default_mask=MASK_HASH,
        rules=(
            Rule(
                exact=_set(["dob", "dateofbirth", "birthdate", "birthday"]),
                suffixes=("dateofbirth", "birthdate"),
            ),
        ),
        partial=PARTIAL_YEAR,
        partial_example=("1984-07-21", "1984-**-**"),
        examples=("dob", "date_of_birth", "birthdate"),
    ),
    Kind(
        id="device_id",
        label="Device identifiers",
        description="Device, advertising and hardware identifiers, IMEI, MAC addresses",
        default_mask=MASK_HASH,
        rules=(
            Rule(
                exact=_set(
                    [
                        "deviceid", "imei", "imsi", "meid", "macaddress", "mac", "hwid", "hardwareid", "androidid",
                        "advertisingid", "idfa", "idfv", "gaid", "adid", "udid",
                    ]
                ),
            ),
        ),
        partial=PARTIAL_LAST_CHARACTERS,
        partial_example=("35-209900-176148-1", "****1481"),
        examples=("device_id", "imei", "mac_address"),
    ),
    Kind(
        id="secret",
        label="Passwords, tokens and keys",
        description="Passwords, API keys, access tokens, private keys, card security codes, one-time codes",
        default_mask=MASK_REDACT,
        rules=(
            Rule(
                exact=_set(
                    [
                        "password", "passwd", "pwd", "passphrase", "secret", "secretkey", "clientsecret", "apikey",
                        "apisecret", "accesstoken", "refreshtoken", "idtoken", "authtoken", "bearertoken",
                        "sessiontoken", "csrftoken", "privatekey", "encryptionkey", "signingkey", "cvv", "cvc",
                        "cvv2", "cvc2", "securitycode", "cardverificationvalue", "pin", "otp", "otpcode",
                        "onetimecode", "mfacode", "totp", "recoverycode", "securityanswer",
                    ]
                ),
                suffixes=("password", "passwd", "secret", "apikey", "privatekey", "accesstoken", "refreshtoken"),
            ),
        ),
        partial=PARTIAL_NOTHING,
        partial_example=("correct horse battery staple", "****"),
        # A hashed password is a cracking target.
        masks=(MASK_PARTIAL, MASK_REDACT),
        inherit_all=True,
        examples=("password", "api_key", "access_token"),
    ),
)

KINDS_BY_ID: dict[str, Kind] = {kind.id: kind for kind in KINDS}
KIND_IDS: tuple[str, ...] = tuple(kind.id for kind in KINDS)

#: The deployment default as it ships (D4): emails, phone numbers and national identifiers hashed,
#: secrets withheld. Adjustable on the Settings page (`default_protection`).
DEFAULT_PROTECTION: dict[str, str] = {"email": MASK_HASH, "phone": MASK_HASH, "national_id": MASK_HASH, "secret": MASK_REDACT}


class ProtectionTextError(ValueError):
    """A protection default's text names an unknown kind, or a mask its kind does not allow."""


def format_protection_text(kinds: dict[str, str]) -> str:
    """A choice of kinds and masks as the setting stores it: `email=hash,phone=partial`, in the
    catalog's order. Empty for nothing protected."""
    return ",".join(f"{kind_id}={kinds[kind_id]}" for kind_id in KIND_IDS if kind_id in kinds)


def parse_protection_text(text: str | None) -> dict[str, str]:
    """The kinds and masks a setting's text names; `ProtectionTextError` for anything invalid.

    `email=hash,secret=redact`; a kind alone takes its default mask (`email,secret`); spaces are
    ignored and an empty text protects nothing.
    """
    kinds: dict[str, str] = {}
    for part in (text or "").split(","):
        item = part.strip()
        if not item:
            continue
        kind_id, _, mask = (piece.strip() for piece in item.partition("="))
        entry = KINDS_BY_ID.get(kind_id)
        if entry is None:
            raise ProtectionTextError(f"{kind_id!r} is not a kind of sensitive data; kinds are: {', '.join(KIND_IDS)}")
        resolved = mask or entry.default_mask
        if resolved not in MASKS:
            raise ProtectionTextError(f"{resolved!r} is not a mask; masks are: {', '.join(MASKS)}")
        if not entry.allows(resolved):
            raise ProtectionTextError(f"{entry.label} cannot be masked with {resolved}; allowed: {', '.join(entry.masks)}")
        kinds[kind_id] = resolved
    return kinds


def normalize_protection_text(text: str | None) -> str:
    """A protection default's text in its stored form; `ProtectionTextError` when invalid."""
    return format_protection_text(parse_protection_text(text))


#: The shipped default as the setting stores it.
DEFAULT_PROTECTION_TEXT = format_protection_text(DEFAULT_PROTECTION)


def kind(kind_id: str) -> Kind:
    """The kind with this id; KeyError when there is none."""
    return KINDS_BY_ID[kind_id]


def kind_options() -> tuple[dict, ...]:
    """The kinds as a setting's options: value, label, description and the masks each allows."""
    return tuple(
        {"value": item.id, "label": item.label, "description": item.description, "choices": list(item.masks)}
        for item in KINDS
    )


def catalog_payload() -> dict:
    """The catalog as the API describes it: every kind and every mask."""
    return {
        "kinds": [item.to_dict() for item in KINDS],
        "masks": [
            {"id": mask, "label": MASK_LABELS[mask], "description": MASK_DESCRIPTIONS[mask]} for mask in MASKS
        ],
    }
