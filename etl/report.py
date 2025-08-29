# etl/report.py
from jinja2 import Template

def generate(df_team, title="Team Scoreboard"):
    """
    Generate a simple HTML report from team scores (DataFrame with 'team' and 'points').
    Includes a basic search filter for demo purposes.
    """
    template_str = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>{{ title }}</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            h1 { color: #333; }
            table { border-collapse: collapse; width: 50%; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: center; }
            th { background-color: #f2f2f2; }
        </style>
    </head>
    <body>
        <h1>{{ title }}</h1>
        <input type="text" id="nameFilter" placeholder="Filter team...">
        <table id="scores">
            <tr><th>Team</th><th>Points</th></tr>
            {% for row in data %}
            <tr><td>{{ row['team'] }}</td><td>{{ row['points'] }}</td></tr>
            {% endfor %}
        </table>
        <script>
        document.getElementById("nameFilter").addEventListener("keyup", function() {
            var filter = this.value.toLowerCase();
            var rows = document.querySelectorAll("#scores tr");
            for (var i = 1; i < rows.length; i++) {
                var name = rows[i].cells[0].textContent.toLowerCase();
                rows[i].style.display = name.includes(filter) ? "" : "none";
            }
        });
        </script>
    </body>
    </html>
    """
    template = Template(template_str)
    return template.render(title=title, data=df_team.to_dict(orient='records'))