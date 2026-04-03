"""HTML page routes."""

from functools import wraps
from flask import Blueprint, render_template, request, session, redirect, url_for, jsonify

pages_bp = Blueprint("pages", __name__)

# ── Hardcoded credentials ──────────────────────────────────────────
VALID_USER = "admin"
VALID_PASS = "admin852"


def login_required(f):
    """Decorator: redirect to /login if not authenticated."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("pages.login_page"))
        return f(*args, **kwargs)
    return decorated


# ── Login / Logout ─────────────────────────────────────────────────
@pages_bp.route("/login", methods=["GET"])
def login_page():
    if session.get("logged_in"):
        return redirect(url_for("pages.index"))
    return render_template("login.html")


@pages_bp.route("/login", methods=["POST"])
def login_submit():
    data = request.get_json(silent=True) or {}
    username = data.get("username", "")
    password = data.get("password", "")

    if username == VALID_USER and password == VALID_PASS:
        session["logged_in"] = True
        session["user"] = username
        return jsonify(success=True)
    return jsonify(success=False, message="Invalid username or password"), 401


@pages_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("pages.login_page"))


# ── Protected pages ────────────────────────────────────────────────
@pages_bp.route("/")
@login_required
def index():
    return render_template("index.html")


@pages_bp.route("/stock/<symbol>")
@login_required
def stock_detail(symbol):
    return render_template("stock_detail.html", symbol=symbol.upper())


@pages_bp.route("/portfolio")
@pages_bp.route("/portfolio/<int:pid>")
@login_required
def portfolio_page(pid=None):
    return render_template("portfolio.html", portfolio_id=pid)
