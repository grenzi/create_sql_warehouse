import PySimpleGUI as sg
import os
import json
import logging

logger = logging.getLogger(__file__)


class Settings:
    settings = None
    must_keys = [
        "source_server",
        "source_db",
        "source_schema",
        "target_server",
        "target_db",
        "staging_schema",
        "temporal_schema",
        "dimension_schema",
        "dimension_id_column_name",
        "outputdir",
        "dropfirst",
    ]

    def __init__(self, *args, **kwargs):
        logger.info(f"looking for settings here: {kwargs.get('config_path')} ")
        self.settingspath = os.path.join(kwargs.get("config_path") or "settings.json")
        if os.path.exists(self.settingspath):
            logger.debug("loading from file")
            with open(self.settingspath, "r", encoding="utf-8") as fp:
                self.settings = json.load(fp)
        else:
            logger.debug("file not loaded")
            self.settings = {}

        allmustpresent = True
        for key in self.must_keys:
            if key not in self.settings.keys():
                logger.warn(f"{key} not found in {self.settingspath}")
                allmustpresent = False

        if not allmustpresent:
            logger.debug("prompting")
            self._get_settings(self.settingspath)

    def get(self, *args, **kwargs):
        key = args[0]
        if self.settings.get(key) is None:
            self.settings[key] = getattr(self, "_get_" + key)(**kwargs)
        return self.settings.get(key)

    def _save_settings(self, settings):
        with open(os.path.join(self.settingspath), "w", encoding="utf-8") as fp:
            json.dump(settings, fp, indent=4, sort_keys=True)

    def _get_settings(self, config_path):
        self.settings_layout = [
            [sg.Text("Basic Settings")],
            [
                sg.Text("Source Server", size=(15, 1)),
                sg.InputText(self.settings["source_server"], key="source_server"),
            ],
            [
                sg.Text("Source Schema", size=(15, 1)),
                sg.InputText(self.settings["source_schema"], key="source_schema"),
            ],
            [
                sg.Text("Source Database", size=(15, 1)),
                sg.InputText(self.settings["source_db"], key="source_db"),
            ],
            [
                sg.Text("Target Server", size=(15, 1)),
                sg.InputText(self.settings["target_server"], key="target_server"),
            ],
            [
                sg.Text("Target Database", size=(15, 1)),
                sg.InputText(self.settings["target_db"], key="target_db"),
            ],
            [
                sg.Text("Staging Schema", size=(15, 1)),
                sg.InputText(self.settings["staging_schema"], key="staging_schema"),
            ],
            [
                sg.Text("Temporal Schema", size=(15, 1)),
                sg.InputText(self.settings["temporal_schema"], key="temporal_schema"),
            ],
            [
                sg.Text("Dimension Schema", size=(15, 1)),
                sg.InputText(self.settings["dimension_schema"], key="dimension_schema"),
            ],
            [
                sg.Text("Dimension Id Column", size=(15, 1)),
                sg.InputText(
                    self.settings["dimension_id_column_name"],
                    key="dimension_id_column_name",
                ),
            ],
            [
                sg.Text("Output Directory", size=(15, 1)),
                sg.InputText(self.settings["outputdir"], key="outputdir"),
            ],
            [
                sg.Checkbox(
                    "Generate Drop Statements?",
                    default=self.settings["dropfirst"],
                    key="dropfirst",
                )
            ],
            [sg.Checkbox("Save Defaults? ", default=False, key="save")],
            [sg.Ok(), sg.Cancel()],
        ]
        window = sg.Window("self.settings", self.settings_layout)
        event, values = window.Read()
        window.Close()

        if event == "Cancel":
            print("cancel pressed. exiting.")
            exit(code=-1)

        if values["save"]:
            del values["save"]
            _save_self.settings(values)

        return values

    def _get_source_tables(self, tablelist):
        layout = [
            [
                sg.Listbox(
                    values=tablelist,
                    select_mode=sg.LISTBOX_SELECT_MODE_MULTIPLE,
                    size=(40, min(len(tablelist), 40)),
                )
            ],
            [sg.OK()],
        ]

        window = sg.Window("Select Input Tables", layout)
        event, values = window.Read()
        window.Close()
        return values[0]

    def _get_primary_keys(self, columns):
        tablelist = list([x.name for x in columns])
        layout = [
            [
                sg.Listbox(
                    values=tablelist,
                    select_mode=sg.LISTBOX_SELECT_MODE_MULTIPLE,
                    size=(40, min(len(tablelist), 40)),
                )
            ],
            [sg.OK()],
        ]
        window = sg.Window("No PK Found. Select Primary Keys", layout)
        event, values = window.Read()
        window.Close()
        return values[0]

    def _get_source_primary_keys(self, columns):
        return self._get_primary_keys(self, columns)

    def _get_staging_primary_keys(self, columns):
        return self._get_primary_keys(self, columns=columns)

    def _get_staging_columns(self, columns):
        # select source tables
        layout = [
            [
                sg.Listbox(
                    values=columns,
                    default_values=columns,
                    select_mode=sg.LISTBOX_SELECT_MODE_MULTIPLE,
                    size=(80, min(len(columns), 40)),
                )
            ],
            [sg.OK()],
        ]
        window = sg.Window("Select Columns to Include in Staging", layout)
        event, values = window.Read()
        window.Close()
        return values[0]

    def _get_scd_type(self):
        layout = [[sg.Listbox(values=["Type 1", "Type 2"], size=(40, 15))], [sg.OK()]]
        window = sg.Window("Select Slowly Changing Dimention Type", layout)
        event, values = window.Read()
        window.Close()
        return values[0][0]

    def _get_scd_columns(self, columns):
        layout = [
            [
                sg.Listbox(
                    values=columns,
                    default_values=columns,
                    select_mode=sg.LISTBOX_SELECT_MODE_MULTIPLE,
                    size=(80, min(len(columns), 40)),
                )
            ],
            [sg.OK()],
        ]
        window = sg.Window(
            "Select Columns to include in slowly changing dimension", layout
        )
        event, values = window.Read()
        window.Close()
        return list([v for v in values[0]])
