import PySimpleGUI as sg
import os
import json


def _load_settings():
    settingspath = os.path.join("settings.json")
    keys = [
        "source_server",
        "source_db",
        "source_schema",
        "source_table",
        "target_server",
        "target_db",
        "staging_schema",
        "temporal_schema",
        "dimension_schema",
        "dimension_id_column_name",
        "outputdir",
        "dropfirst",
    ]
    if os.path.exists(settingspath):
        with open(settingspath, "r", encoding="utf-8") as fp:
            settings = json.load(fp)
    else:
        settings = {}
        for k in keys:
            settings[k] = None

    return settings


def _save_settings(settings):
    with open(os.path.join("settings.json"), "w", encoding="utf-8") as fp:
        json.dump(settings, fp, indent=4, sort_keys=True)


def get_settings():
    settings = _load_settings()
    settings_layout = [
        [sg.Text("Settings")],
        [
            sg.Text("Source Server", size=(15, 1)),
            sg.InputText(settings["source_server"], key="source_server"),
        ],
        [
            sg.Text("Source Schema", size=(15, 1)),
            sg.InputText(settings["source_schema"], key="source_schema"),
        ],
        [
            sg.Text("Source Database", size=(15, 1)),
            sg.InputText(settings["source_db"], key="source_db"),
        ],
        [
            sg.Text("Target Server", size=(15, 1)),
            sg.InputText(settings["target_server"], key="target_server"),
        ],
        [
            sg.Text("Target Database", size=(15, 1)),
            sg.InputText(settings["target_db"], key="target_db"),
        ],
        [
            sg.Text("Staging Schema", size=(15, 1)),
            sg.InputText(settings["staging_schema"], key="staging_schema"),
        ],
        [
            sg.Text("Temporal Schema", size=(15, 1)),
            sg.InputText(settings["temporal_schema"], key="temporal_schema"),
        ],
        [
            sg.Text("Dimension Schema", size=(15, 1)),
            sg.InputText(settings["dimension_schema"], key="dimension_schema"),
        ],
        [
            sg.Text("Dimension Id Column", size=(15, 1)),
            sg.InputText(
                settings["dimension_id_column_name"], key="dimension_id_column_name"
            ),
        ],
        [
            sg.Text("Output Directory", size=(15, 1)),
            sg.InputText(settings["outputdir"], key="outputdir"),
        ],
        [
            sg.Checkbox(
                "Generate Drop Statements?",
                default=settings["dropfirst"],
                key="dropfirst",
            )
        ],
        [sg.Checkbox("Save Defaults? ", default=False, key="save")],
        [sg.Ok(), sg.Cancel()],
    ]
    window = sg.Window("Settings", settings_layout)
    event, values = window.Read()
    window.Close()

    if event == "Cancel":
        print("cancel pressed. exiting.")
        exit(code=-1)

    if values["save"]:
        del values["save"]
        _save_settings(values)

    return values
