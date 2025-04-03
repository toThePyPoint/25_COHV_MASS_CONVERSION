import tkinter as tk
from tkinter import messagebox, BooleanVar


# Function to display message box
def show_message(msg_text):
    root = tk.Tk()  # Create a root window
    root.withdraw()  # Hide the root window
    messagebox.showinfo("Message", msg_text)  # Display the message box
    root.destroy()  # Close the root window when the message box is closed


class OptionSelector:
    def __init__(self, options, title="Select an Option"):
        self.options = options
        self.title = title
        self.user_choice = None

    def get_user_choice(self):
        self.user_choice = self.option_var.get()  # Store the selected option
        self.root.destroy()  # Close the popup

    def show(self):
        # Create the main window
        self.root = tk.Tk()
        self.root.title(self.title)
        self.root.geometry("300x500")

        # Create a StringVar to hold the user's choice
        self.option_var = tk.StringVar(value=self.options[0])  # Default selection

        # Create labels and radio buttons
        label = tk.Label(self.root, text="Please, select the file from which data should be taken.")
        label.pack(pady=10)

        message = tk.Label(self.root, text="And confirm with 'OK' button.")
        message.pack(pady=5)

        for option in self.options:
            radio = tk.Radiobutton(self.root, text=option, variable=self.option_var, value=option)
            radio.pack(anchor="w", padx=20)

        # Create an OK button
        ok_button = tk.Button(self.root, text="OK", command=self.get_user_choice)
        ok_button.pack(pady=20)

        # Run the Tkinter event loop
        self.root.mainloop()

        # Return the user's choice
        return self.user_choice


class OptionMultiSelector:
    def __init__(self, options, title="Select Options"):
        self.options = options
        self.title = title
        self.user_choices = []

    def get_user_choices(self):
        self.user_choices = [option for option, var in self.option_vars.items() if var.get()]
        self.root.destroy()

    def show(self):
        # Create the main window
        self.root = tk.Tk()
        self.root.title(self.title)
        self.root.geometry("300x500")

        # Create a dictionary of BooleanVars for checkboxes
        self.option_vars = {option: BooleanVar(value=(i == 0)) for i, option in enumerate(self.options)}

        # Create labels and checkboxes
        label = tk.Label(self.root, text="Please, select the files from which data should be taken.")
        label.pack(pady=10)

        message = tk.Label(self.root, text="And confirm with 'OK' button.")
        message.pack(pady=5)

        for option, var in self.option_vars.items():
            checkbox = tk.Checkbutton(self.root, text=option, variable=var)
            checkbox.pack(anchor="w", padx=20)

        # Create an OK button
        ok_button = tk.Button(self.root, text="OK", command=self.get_user_choices)
        ok_button.pack(pady=20)

        # Run the Tkinter event loop
        self.root.mainloop()

        # Return the user's choices
        return self.user_choices

