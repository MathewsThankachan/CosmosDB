namespace BulkInsert
{
    partial class Form1
    {
        /// <summary>
        /// Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        /// Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        /// Required method for Designer support - do not modify
        /// the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            this.btnBulkInsert = new System.Windows.Forms.Button();
            this.btnBlobStorageBulkInsert = new System.Windows.Forms.Button();
            this.SuspendLayout();
            // 
            // btnBulkInsert
            // 
            this.btnBulkInsert.Location = new System.Drawing.Point(214, 158);
            this.btnBulkInsert.Name = "btnBulkInsert";
            this.btnBulkInsert.Size = new System.Drawing.Size(410, 127);
            this.btnBulkInsert.TabIndex = 0;
            this.btnBulkInsert.Text = "BulkInsert - Local";
            this.btnBulkInsert.UseVisualStyleBackColor = true;
            this.btnBulkInsert.Click += new System.EventHandler(this.btnBulkInsert_Click);
            // 
            // btnBlobStorageBulkInsert
            // 
            this.btnBlobStorageBulkInsert.Location = new System.Drawing.Point(750, 158);
            this.btnBlobStorageBulkInsert.Name = "btnBlobStorageBulkInsert";
            this.btnBlobStorageBulkInsert.Size = new System.Drawing.Size(410, 127);
            this.btnBlobStorageBulkInsert.TabIndex = 1;
            this.btnBlobStorageBulkInsert.Text = "BulkInsert-Blob Storage";
            this.btnBlobStorageBulkInsert.UseVisualStyleBackColor = true;
            this.btnBlobStorageBulkInsert.Click += new System.EventHandler(this.btnBlobStorageBulkInsert_Click);
            // 
            // Form1
            // 
            this.ClientSize = new System.Drawing.Size(1559, 517);
            this.Controls.Add(this.btnBlobStorageBulkInsert);
            this.Controls.Add(this.btnBulkInsert);
            this.MaximizeBox = false;
            this.MinimizeBox = false;
            this.Name = "Form1";
            this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
            this.Load += new System.EventHandler(this.Form1_Load);
            this.ResumeLayout(false);

        }

        #endregion


        private System.Windows.Forms.Button btnBulkInsert;
        private System.Windows.Forms.Button btnBlobStorageBulkInsert;
    }
}

