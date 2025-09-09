import React from 'react';
import {
  Container,
  Card,
  CardContent,
  Typography,
  Box,
  TextField,
  FormControlLabel,
  Checkbox,
  Link,
  Divider,
  Alert,
} from '@mui/material';
import { Login as LoginIcon, PersonAdd } from '@mui/icons-material';
import { motion } from 'framer-motion';
import { useForm, Controller } from 'react-hook-form';
import { useNavigate, useLocation } from 'react-router-dom';
import { Button } from '@/components/core';
import { useUserAuth } from '@/users/hooks';
import { LoginCredentials } from '@/users/types';
import { validateEmail } from '@/users/utils';

const Login: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { login, isLoggingIn } = useUserAuth();

  const from = location.state?.from?.pathname || '/dashboard';

  const {
    control,
    handleSubmit,
    formState: { errors },
    setError,
  } = useForm<LoginCredentials>({
    defaultValues: {
      email: '',
      password: '',
      remember_me: false,
    },
  });

  const handleFormSubmit = async (data: LoginCredentials) => {
    try {
      await login(data);
      navigate(from, { replace: true });
    } catch (error: any) {
      if (error?.response?.data?.code === 'password_expired') {
        navigate('/users/change-password');
      } else if (
        error?.response?.data?.error?.includes('Please verify your email address before logging in') ||
        error?.response?.data?.non_field_errors?.some((msg: string) => 
          msg.includes('Please verify your email address before logging in')
        )
      ) {
        navigate('/verify-email', { state: { email: data.email } });
      } else if (error?.response?.data?.error) {
        setError('root', { message: error.response.data.error });
      } else if (error?.response?.data?.non_field_errors) {
        setError('root', { message: error.response.data.non_field_errors[0] });
      }
    }
  };

  return (
    <Container maxWidth="sm">
      <Box
        sx={{
          minHeight: '100vh',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}
      >
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.5 }}
          style={{ width: '100%' }}
        >
          <Card>
            <CardContent sx={{ p: 4 }}>
              <Box sx={{ textAlign: 'center', mb: 4 }}>
                <LoginIcon sx={{ fontSize: 60, color: 'primary.main', mb: 2 }} />
                <Typography variant="h4" gutterBottom>
                  Welcome Back
                </Typography>
                <Typography variant="body1" color="text.secondary">
                  Sign in to your NovaMeet account
                </Typography>
              </Box>

              {errors.root && (
                <Alert severity="error" sx={{ mb: 3 }}>
                  {errors.root.message}
                </Alert>
              )}

              <Box component="form" onSubmit={handleSubmit(handleFormSubmit)}>
                <Controller
                  name="email"
                  control={control}
                  rules={{
                    required: 'Email is required',
                    validate: (value) => validateEmail(value) || 'Please enter a valid email address',
                  }}
                  render={({ field }) => (
                    <TextField
                      {...field}
                      fullWidth
                      label="Email Address"
                      type="email"
                      margin="normal"
                      error={!!errors.email}
                      helperText={errors.email?.message}
                      autoFocus
                    />
                  )}
                />

                <Controller
                  name="password"
                  control={control}
                  rules={{ required: 'Password is required' }}
                  render={({ field }) => (
                    <TextField
                      {...field}
                      fullWidth
                      label="Password"
                      type="password"
                      margin="normal"
                      error={!!errors.password}
                      helperText={errors.password?.message}
                    />
                  )}
                />

                <Controller
                  name="remember_me"
                  control={control}
                  render={({ field }) => (
                    <FormControlLabel
                      control={<Checkbox {...field} checked={field.value} />}
                      label="Remember me for 30 days"
                      sx={{ mt: 1, mb: 2 }}
                    />
                  )}
                />

                <Button
                  type="submit"
                  fullWidth
                  variant="contained"
                  size="large"
                  loading={isLoggingIn}
                  loadingText="Signing in..."
                  sx={{ mt: 3, mb: 2 }}
                >
                  Sign In
                </Button>

                <Box sx={{ textAlign: 'center', mb: 2 }}>
                  <Link
                    component="button"
                    type="button"
                    variant="body2"
                    onClick={() => navigate('/request-password-reset')}
                  >
                    Forgot your password?
                  </Link>
                </Box>

                <Divider sx={{ my: 3 }}>
                  <Typography variant="body2" color="text.secondary">
                    or
                  </Typography>
                </Divider>

                <Button
                  fullWidth
                  variant="outlined"
                  size="large"
                  startIcon={<PersonAdd />}
                  onClick={() => navigate('/register')}
                >
                  Create New Account
                </Button>
              </Box>
            </CardContent>
          </Card>
        </motion.div>
      </Box>
    </Container>
  );
};

export default Login;